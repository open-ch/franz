package franz

import (
	"fmt"
	"reflect"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// getAcls is the command run by kafka acls list
func (f *Franz) GetAcls() (KafkaACLs, error) {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return KafkaACLs{}, err
	}

	// Get all Kafka ACLs
	f.log.Info("getting Kafka ACLs")
	acls, err := clusterAdmin.GetTransformedAcls()
	if err != nil {
		return KafkaACLs{}, errors.Wrap(err, "cannot get Kafka ACLs")
	}

	return acls, nil
}

// setAcls is the command run by kafka acls setAcls
func (f *Franz) SetACLs(diff ACLDiff) error {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return err
	}

	err = clusterAdmin.SetKafkaAcls(diff)
	if err != nil {
		return errors.Wrap(err, "failed to set ACLs")
	}

	return nil
}

// GetACLsDiff returns a diff between the passed in ACLs and the current configured ACLs
func (f *Franz) GetACLsDiff(kafkaACLs KafkaACLs) (ACLDiff, error) {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return ACLDiff{}, err
	}

	if !ValidateACLs(kafkaACLs) {
		return ACLDiff{}, errors.New("ACLs validation failed: file contains duplicate ACLs")
	}

	return clusterAdmin.GetACLsDiff(kafkaACLs)

}

// GetTransformedAcls returns the current ACL configuration in franz native form
func (c *ClusterAdmin) GetTransformedAcls() (KafkaACLs, error) {
	acls, err := c.getAcls()
	if err != nil {
		return KafkaACLs{}, err
	}

	return saramaToKafkaResources(acls), nil
}

// getAcls returns an array of all existing ACLs in Kafka
func (c *ClusterAdmin) getAcls() ([]sarama.ResourceAcls, error) {
	// generic filter to match all ACLs
	filter := sarama.AclFilter{
		ResourceType:              sarama.AclResourceAny,
		Operation:                 sarama.AclOperationAny,
		PermissionType:            sarama.AclPermissionAny,
		ResourcePatternTypeFilter: sarama.AclPatternAny,
	}
	resourcesACLs, err := c.client.ListAcls(filter)

	if err != nil {
		c.log.Printf("Failed to retrieve ACLs: %s", err)
		return nil, err
	}

	return resourcesACLs, nil
}

// SetKafkaAcls takes a list of resource ACLs and does the appropriate actions (create, delete) to install these ACLs in Kafka
func (c *ClusterAdmin) SetKafkaAcls(diff ACLDiff) error {
	toCreate := diff.ToCreate
	toDelete := diff.ToDelete

	if len(toDelete) > 0 {
		if err := c.deleteAcls(toDelete); err != nil {
			return errors.Wrap(err, "failed to delete the ACLs")
		}
	}

	if len(toCreate) > 0 {
		if err := c.createAcls(toCreate); err != nil {
			return errors.Wrap(err, "failed to create the ACLs")
		}
	}

	return nil
}

// ACLDiff holds the diff in sarama form
type ACLDiff struct {
	ToCreate []sarama.ResourceAcls
	ToDelete []sarama.ResourceAcls
}

// ACLDiffTransformed holds the diff in transformed, franz native form
type ACLDiffTransformed struct {
	ToCreate KafkaACLs `json:"to_create" yaml:"to_create"`
	ToDelete KafkaACLs `json:"to_delete" yaml:"to_delete"`
}

// Transform transforms from sarama to franz native form.
func (a ACLDiff) Transform() ACLDiffTransformed {
	return ACLDiffTransformed{
		ToCreate: saramaToKafkaResources(a.ToCreate),
		ToDelete: saramaToKafkaResources(a.ToDelete),
	}
}

// GetACLsDiff returns a diff between the passed in ACLs and the current configured ACLs
func (c *ClusterAdmin) GetACLsDiff(kafkaAcls KafkaACLs) (ACLDiff, error) {
	resAclsExisting, err := c.getAcls()
	if err != nil {
		return ACLDiff{}, errors.Wrap(err, "failed to retrieve the existing ACLs")
	}

	resAclsNew := kafkaToSaramaResources(kafkaAcls)
	toCreate, toDelete := diffResourcesACLs(resAclsNew, resAclsExisting)

	return ACLDiff{
		ToCreate: toCreate,
		ToDelete: toDelete,
	}, nil
}

// deleteAcls deletes all ACLs that are referenced in the given ResourceAcls object
func (c *ClusterAdmin) deleteAcls(resourcesAcls []sarama.ResourceAcls) (err error) {
	for _, resourceAcls := range resourcesAcls {
		acls := resourceAcls.Acls
		for _, acl := range acls {
			filter := sarama.AclFilter{
				ResourceType:              resourceAcls.ResourceType,
				ResourceName:              &resourceAcls.ResourceName,
				ResourcePatternTypeFilter: resourceAcls.ResourcePatternType,
				Principal:                 &acl.Principal,
				Host:                      &acl.Host,
				Operation:                 acl.Operation,
				PermissionType:            acl.PermissionType,
			}
			matchingACLs, err := c.client.DeleteACL(filter, false)
			if err != nil {
				return err
			} else if matchingACLs != nil {
				// matchingACLs should contain just one deleted ACL
				c.log.Printf("Deleted ACL: %s", aclToString(&matchingACLs[0].Resource, &matchingACLs[0].Acl))
			} else {
				c.log.Printf("No ACL could be matched by the filter, no ACL was deleted. Probably the filter is wrong")
			}
		}
	}
	return nil
}

// createAcls creates the ACLs in the passed array
func (c *ClusterAdmin) createAcls(resourcesAcls []sarama.ResourceAcls) error {
	for _, resourceAcls := range resourcesAcls {
		acls := resourceAcls.Acls
		for _, acl := range acls {
			err := c.client.CreateACL(resourceAcls.Resource, *acl)
			if err != nil {
				return err
			}

			c.log.Printf("Created ACL: %s", aclToString(&resourceAcls.Resource, acl))
		}
	}

	return nil
}

var OperationCode = map[string]sarama.AclOperation{
	"Unknown":         sarama.AclOperationUnknown,
	"Any":             sarama.AclOperationAny,
	"All":             sarama.AclOperationAll,
	"Read":            sarama.AclOperationRead,
	"Write":           sarama.AclOperationWrite,
	"Create":          sarama.AclOperationCreate,
	"Delete":          sarama.AclOperationDelete,
	"Alter":           sarama.AclOperationAlter,
	"Describe":        sarama.AclOperationDescribe,
	"CusterAction":    sarama.AclOperationClusterAction,
	"DescribeConfigs": sarama.AclOperationDescribeConfigs,
	"AlterConfigs":    sarama.AclOperationAlterConfigs,
	"IdempotentWrite": sarama.AclOperationIdempotentWrite,
}

func operationName(code sarama.AclOperation) (name string) {
	for k, v := range OperationCode {
		if v == code {
			return k
		}
	}
	return
}

var PermissionCode = map[string]sarama.AclPermissionType{
	"Unknown": sarama.AclPermissionUnknown,
	"Any":     sarama.AclPermissionAny,
	"Deny":    sarama.AclPermissionDeny,
	"Allow":   sarama.AclPermissionAllow,
}

func permissionName(code sarama.AclPermissionType) (name string) {
	for k, v := range PermissionCode {
		if v == code {
			return k
		}
	}
	return
}

var resourceCode = map[string]sarama.AclResourceType{
	"Topic":   sarama.AclResourceTopic,
	"Cluster": sarama.AclResourceCluster,
	"Group":   sarama.AclResourceGroup,
}

func resourceName(code sarama.AclResourceType) (name string, ok bool) {
	for k, v := range resourceCode {
		if v == code {
			return k, true
		}
	}
	return
}

var resourcePatternTypeCode = map[string]sarama.AclResourcePatternType{
	"Unknown":  sarama.AclPatternUnknown,
	"Any":      sarama.AclPatternAny,
	"Match":    sarama.AclPatternMatch,
	"Literal":  sarama.AclPatternLiteral,
	"Prefixed": sarama.AclPatternPrefixed,
}

func resourcePatternTypeName(typ sarama.AclResourcePatternType) (name string) {
	for k, v := range resourcePatternTypeCode {
		if v == typ {
			return k
		}
	}
	return
}

type KafkaACLs struct {
	Cluster       []ACLs
	ConsumerGroup []ACLs
	Topic         []ACLs
}

// ACLs represents a versioned set of access control lists for a kafka resource.
// TODO: maybe name it ResourceACLs to align it with sarama
type ACLs struct {
	Version     int    `json:"version" yaml:"-"`
	Name        string `json:"name" yaml:"name"`
	PatternType string `json:"pattern_type" yaml:"resourcePatternType,omitempty"` // optional, exists only for kafka version 2.1 and above
	ACL         []ACL  `json:"acls" yaml:"acl"`
}

// ACL represents a single ACL
type ACL struct {
	Host           string `json:"host"`
	Operation      string `json:"operation"`
	PermissionType string `json:"permissionType" yaml:"action"`
	Principal      string `json:"principal"`
}

func kafkaToSaramaResources(kafkaACLs KafkaACLs) []sarama.ResourceAcls {
	var saramaContainer []sarama.ResourceAcls
	saramaContainer = append(saramaContainer, toSaramaResourcesACLs(kafkaACLs.Cluster, "Cluster")...)
	saramaContainer = append(saramaContainer, toSaramaResourcesACLs(kafkaACLs.ConsumerGroup, "Group")...)
	saramaContainer = append(saramaContainer, toSaramaResourcesACLs(kafkaACLs.Topic, "Topic")...)

	return saramaContainer
}

func saramaToKafkaResources(saramaContainer []sarama.ResourceAcls) KafkaACLs {
	var cluster, group, topic []ACLs
	for _, resourceAcls := range saramaContainer {
		resourceType := resourceAcls.ResourceType
		acls := saramaToKafkaAcls(resourceAcls)
		acls.Name = resourceAcls.ResourceName
		// the pattern type is available on kafka 2.0.0 and above. On lower versions it will be set as "Unknown", in which case we ignore the field.
		if resourceAcls.ResourcePatternType != sarama.AclPatternUnknown {
			acls.PatternType = resourcePatternTypeName(resourceAcls.ResourcePatternType)
		}
		// TODO: Default value, verify this
		acls.Version = 1
		if resourceType == sarama.AclResourceCluster {
			cluster = append(cluster, acls)
		} else if resourceType == sarama.AclResourceGroup {
			group = append(group, acls)
		} else if resourceType == sarama.AclResourceTopic {
			topic = append(topic, acls)
		}
	}

	return KafkaACLs{
		Cluster:       cluster,
		ConsumerGroup: group,
		Topic:         topic,
	}
}

func saramaToKafkaAcls(saramaResourceAcls sarama.ResourceAcls) (acls ACLs) {
	acls.Name = saramaResourceAcls.ResourceName
	for _, saramaACL := range saramaResourceAcls.Acls {
		acl := ACL{}
		acl.Principal = saramaACL.Principal
		acl.Host = saramaACL.Host
		acl.Operation = operationName(saramaACL.Operation)
		acl.PermissionType = permissionName(saramaACL.PermissionType)
		acls.ACL = append(acls.ACL, acl)
	}
	return acls
}

// Converts an ACLs object to a sarama Resource ACLs object
func toSaramaResourcesACLs(aclsArray []ACLs, resourceType string) (saramaResources []sarama.ResourceAcls) {
	for _, acls := range aclsArray {
		resourceAcls := sarama.ResourceAcls{}
		resourceAcls.ResourceType = resourceCode[resourceType]
		resourceAcls.ResourceName = acls.Name
		resourceAcls.ResourcePatternType = resourcePatternTypeCode[acls.PatternType]

		for _, acl := range acls.ACL {
			operation := OperationCode[acl.Operation]
			permissionType := PermissionCode[acl.PermissionType]
			acl := sarama.Acl{Principal: acl.Principal, Host: acl.Host, Operation: operation, PermissionType: permissionType}
			resourceAcls.Acls = append(resourceAcls.Acls, &acl)
		}
		saramaResources = append(saramaResources, resourceAcls)
	}
	return saramaResources
}

// diffResourcesACLs goes through the new and existing resourceAcl objects and figures out which policies have to be created and which have to be deleted
func diffResourcesACLs(resAclsNew, resAclsExisting []sarama.ResourceAcls) (resAclsToCreate, resAclsToDelete []sarama.ResourceAcls) {
	var existingResourcesToDelete []sarama.ResourceAcls
	existingResourcesToDelete = append(existingResourcesToDelete, resAclsExisting...)

	for _, resAcls := range resAclsNew {
		index := resourceAclsIndex(resAcls, resAclsExisting)
		if index == -1 {
			// this is a new (new resource type, name, pattern type) set of ACLs to be created
			resAclsToCreate = append(resAclsToCreate, resAcls)
		} else if reflect.DeepEqual(resAcls, resAclsExisting[index]) {
			// this set of ACLs already exists - no ACL needs to be created
			// since this exists in the new acls list we should not delete it
			// (i.e. we must remove it from the array of the 'existing but marked for deletion' resources)
			existingResourcesToDelete = delResourceFromArray(resAcls, existingResourcesToDelete)
		} else if index >= 0 {
			// the resourceAcls object exists but there are differences in the contained ACLs.
			// depending on the differences, we must create or delete ACLs
			aclsToCreate, aclsToDelete := diffACLs(resAcls, resAclsExisting[index])
			if len(aclsToCreate.Acls) > 0 {
				resAclsToCreate = append(resAclsToCreate, *aclsToCreate)
			} else if len(aclsToDelete.Acls) > 0 {
				resAclsToDelete = append(resAclsToDelete, *aclsToDelete)
			}

			if resourceAclsIndex(resAcls, resAclsExisting) == -1 {
				fmt.Printf("This case should not happen, FIX THE CONDITIONAL!")
			}
			// the resource object exists in the new config so we remove it from the ones marked for deletion
			// of course, inside the resource object there might exist ACLs to be deleted. This is handled above in the resAclsToDelete array.
			existingResourcesToDelete = delResourceFromArray(resAcls, existingResourcesToDelete)
		}
	}
	// The resourceAcls items that are left are the ACLs that must be deleted since they are not in the new list
	if len(existingResourcesToDelete) != 0 {
		resAclsToDelete = append(resAclsToDelete, existingResourcesToDelete...)
	}
	return resAclsToCreate, resAclsToDelete
}

// diffACLs computes the ACLs that need to be created and the ones that need to be deleted within the given two ResourceAcls objects.
// It does this by comparing the ACLs loaded from the file with the existing ACLs retrieved from Kafka.
// It is assumed that the two passed ResourceAcls objects refer to the same resource, i.e. they have the same resource type, name and, optionally, resource pattern type.
func diffACLs(newResource, existingResource sarama.ResourceAcls) (toAdd, toRemove *sarama.ResourceAcls) {
	toAdd = &sarama.ResourceAcls{}
	toRemove = &sarama.ResourceAcls{}

	// here it is assumed that both ResourceACLs objects refer to the same resource (resource name, resource type, resource pattern type)
	toAdd.ResourceName = newResource.ResourceName
	toAdd.ResourceType = newResource.ResourceType
	toAdd.ResourcePatternType = newResource.ResourcePatternType
	toRemove.ResourceName = newResource.ResourceName
	toRemove.ResourceType = newResource.ResourceType
	toRemove.ResourcePatternType = newResource.ResourcePatternType

	toAdd.Acls = append(toAdd.Acls, newResource.Acls...)
	// All existing ACLs are initially candidates to be removed
	// By iterating the new ACLs below, we are removing from this array the ones that should not be removed
	toRemove.Acls = append(toRemove.Acls, existingResource.Acls...)

	for _, acl := range newResource.Acls {
		contains, _ := containsACL(existingResource.Acls, *acl)
		// This acl from the new list of ACLs is in the existing list of ACLs as well. Thus, it should be kept as is (no addition and no removal)
		if contains {
			toAdd.Acls = delACLFromArray(*acl, toAdd.Acls)
			toRemove.Acls = delACLFromArray(*acl, toRemove.Acls)
		}
	}
	return toAdd, toRemove
}

func delResourceFromArray(resource sarama.ResourceAcls, resources []sarama.ResourceAcls) []sarama.ResourceAcls {
	index := resourceAclsIndex(resource, resources)
	if index >= 0 {
		resources = append(resources[:index], resources[index+1:]...)
	}
	return resources
}

func delACLFromArray(acl sarama.Acl, acls []*sarama.Acl) []*sarama.Acl {
	index := aclIndex(acl, acls)
	if index == -1 {
		fmt.Printf("The element you are trying to delete does not exist in the array!\n")
	}
	acls = append(acls[:index], acls[index+1:]...)
	return acls
}

func aclIndex(searchACL sarama.Acl, aclList []*sarama.Acl) int {
	_, i := containsACL(aclList, searchACL)
	return i
}

func resourceAclsIndex(resource sarama.ResourceAcls, resources []sarama.ResourceAcls) int {
	_, i := containsResource(resources, resource)
	return i
}

// containsResource searches for the resourceAcls object within an array. If an element with the same name and type is found then it returns true.
func containsResource(resources []sarama.ResourceAcls, resource sarama.ResourceAcls) (exists bool, index int) {
	for i, a := range resources {
		if resource.Resource.ResourceName == a.Resource.ResourceName &&
			resource.Resource.ResourceType == a.Resource.ResourceType &&
			resource.Resource.ResourcePatternType == a.Resource.ResourcePatternType {
			return true, i
		}
	}
	return false, -1
}

// containsACL searches for an ACL within an ACL array.
// If the exact same ACL exists in the array then it returns true and the index of the ACL within the array
func containsACL(aclList []*sarama.Acl, acl sarama.Acl) (exists bool, index int) {
	for i, a := range aclList {
		if a.Principal == acl.Principal &&
			a.Host == acl.Host &&
			a.Operation == acl.Operation &&
			a.PermissionType == acl.PermissionType {
			return true, i
		}
	}
	return false, -1
}

func hashcode(r sarama.ResourceAcls, acl *sarama.Acl) string {
	resourceType, ok := resourceName(r.ResourceType)
	if !ok {
		fmt.Printf("Unknown resource type\n")
	}
	return fmt.Sprintf("%s,%s,%s,%s,%s,%s",
		r.ResourceName,
		resourceType,
		acl.Principal,
		acl.Host,
		permissionName(acl.PermissionType),
		operationName(acl.Operation))
}

// ValidateACLs runs some sanity checks on a list of ACLs
// currently it just checks for duplicated ACL entries
func ValidateACLs(kafkaAcls KafkaACLs) bool {
	resources := kafkaToSaramaResources(kafkaAcls)
	set := make(map[string]bool)
	for _, resource := range resources {
		for _, acl := range resource.Acls {
			_, found := set[hashcode(resource, acl)]
			if found {
				fmt.Printf("Duplicate ACL: %s\n", aclToString(&resource.Resource, acl))
				return false
			}

			set[hashcode(resource, acl)] = true
		}
	}
	return true
}

func aclToString(res *sarama.Resource, acl *sarama.Acl) string {
	resType, ok := resourceName(res.ResourceType)
	if !ok {
		return fmt.Sprintf("Unrecognized resource type: %d\n", res.ResourceType)
	}
	return fmt.Sprintf("Resource Type: %s, Resource Name: %s, Principal: %s, Operation: %s, Permission Type: %s, Host: %s",
		resType, res.ResourceName, acl.Principal, operationName(acl.Operation), permissionName(acl.PermissionType), acl.Host)
}
