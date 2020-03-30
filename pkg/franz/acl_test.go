package franz

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
)

// setup test ACLs
var (
	saramaACL1 = &sarama.Acl{
		Principal:      "principal1",
		Host:           "host1",
		Operation:      sarama.AclOperationWrite,
		PermissionType: sarama.AclPermissionAllow,
	}
	saramaACL2 = &sarama.Acl{
		Principal:      "principal2",
		Host:           "host2",
		Operation:      sarama.AclOperationRead,
		PermissionType: sarama.AclPermissionDeny,
	}
	saramaACL3 = &sarama.Acl{
		Principal:      "principal3",
		Host:           "host3",
		Operation:      sarama.AclOperationCreate,
		PermissionType: sarama.AclPermissionAny,
	}
	saramaACL4 = &sarama.Acl{
		Principal:      "principal3",
		Host:           "host3",
		Operation:      sarama.AclOperationCreate,
		PermissionType: sarama.AclPermissionAny,
	}
	resourceACLs1 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceCluster, ResourceName: "acls1"},
		Acls:     []*sarama.Acl{saramaACL1},
	}
	resourceACLs2 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceGroup, ResourceName: "acls2"},
		Acls:     []*sarama.Acl{saramaACL2},
	}
	resourceACL3 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTopic, ResourceName: "acls3"},
		Acls:     []*sarama.Acl{saramaACL3},
	}
	resourceACL4 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceCluster, ResourceName: "acls1"},
		Acls:     []*sarama.Acl{saramaACL1, saramaACL2},
	}
	resourceACL5 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceGroup, ResourceName: "topic1", ResourcePatternType: sarama.AclPatternPrefixed},
		Acls:     []*sarama.Acl{saramaACL1, saramaACL2},
	}

	// same as above but for Literal pattern type
	resourceACL6 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceGroup, ResourceName: "topic1", ResourcePatternType: sarama.AclPatternLiteral},
		Acls:     []*sarama.Acl{saramaACL1, saramaACL2},
	}

	resourceACL7 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceGroup, ResourceName: "topic1", ResourcePatternType: sarama.AclPatternLiteral},
		Acls:     []*sarama.Acl{saramaACL1, saramaACL2, saramaACL3},
	}

	resourcesACL5 = []sarama.ResourceAcls{resourceACL5}
	resourcesACL6 = []sarama.ResourceAcls{resourceACL6}
	resourcesACL7 = []sarama.ResourceAcls{resourceACL5, resourceACL6}
	resourcesACLs = []sarama.ResourceAcls{resourceACLs1, resourceACLs2, resourceACL3}
)

// Convert from sarama to our representation and back to sarama. We should get an identical object
func TestACLsRepresentationConversion(t *testing.T) {
	resourceACLTest := saramaToKafkaResources(resourcesACLs)
	saramaResources := kafkaToSaramaResources(resourceACLTest)
	if !cmp.Equal(saramaResources, resourcesACLs) {
		t.Log("Failed test: Conversion to the sarama representation")
		t.Fail()
	}
}

func TestDiffResourcesACLs(t *testing.T) {
	// if the two arguments contain the same ACLs then there should be no changes to be implemented
	toCreate, toDelete := diffResourcesACLs(resourcesACLs, resourcesACLs)
	if !(len(toCreate) == 0 && len(toDelete) == 0) {
		t.Log("Failed test: If the new and old files are identical then there should be nothing to implement")
		t.Fail()
	}

	resourcesAcls2 := resourcesACLs[1:]
	toCreate, toDelete = diffResourcesACLs(resourcesAcls2, resourcesACLs)
	if !(len(toCreate) == 0 && len(toDelete) == 1 && cmp.Equal(toDelete[0], resourcesACLs[0])) {
		t.Log("Failed test: If a resourceAcls is removed in the new file then it should be marked for deletion")
		t.Fail()
	}

	// if the new resource type has a different pattern type than an otherwise same existing resource object then we should create a new resource entry
	toCreate, toDelete = diffResourcesACLs(resourcesACL5, resourcesACL6)
	if !(len(toCreate) == 1 && len(toDelete) == 1) &&
		(toCreate[0].ResourcePatternType == sarama.AclPatternPrefixed) {
		t.Log("Failed test: If the new array contains a resource with all fields equal to the existing except the pattern type then it should be considered new and marked for creation")
		t.Fail()
	}

	// same as above but the new array contains the old as well. So, one resource ACL object should be created and nothing marked for deletion
	toCreate, toDelete = diffResourcesACLs(resourcesACL7, resourcesACL5)
	if !(len(toCreate) == 1 && len(toDelete) == 0) {
		t.Log("Failed test: If the new array contains a resource with all fields equal to the existing except the pattern type then it should be considered new and marked for creation")
		t.Fail()
	}

	// if there is an ACL removed from a resourceAcls object then it should be marked for deletion
	// (but not the resourceAcls where it belongs)
	saramaACL := &sarama.Acl{
		Principal:      "principal1",
		Host:           "host1",
		Operation:      sarama.AclOperationWrite,
		PermissionType: sarama.AclPermissionAllow,
	}
	resourceAcls3 := sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceCluster, ResourceName: "acls1"},
		Acls:     []*sarama.Acl{saramaACL},
	}

	resourcesACLs = []sarama.ResourceAcls{resourceACLs1}
	resourcesAcls3 := []sarama.ResourceAcls{resourceAcls3}
	resourcesAcls3[0].Acls = []*sarama.Acl{}
	toDeleteExpected := sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceName: "acls1", ResourceType: sarama.AclResourceCluster},
		Acls:     []*sarama.Acl{saramaACL1},
	}
	toCreate, toDelete = diffResourcesACLs(resourcesAcls3, resourcesACLs)
	if !(len(toCreate) == 0 && len(toDelete) == 1 && cmp.Equal(toDelete[0], toDeleteExpected)) {
		t.Log("Failed test: If  an ACL is removed from the resourceAcls object then it should be marked for deletion (but not the resourceAcls container)")
		t.Fail()
	}
}

func TestDiffACLs(t *testing.T) {
	resource1 := resourceACLs1
	resource2 := resourceACLs2
	toAdd, toRemove := diffACLs(resource1, resource2)
	if !(len(toAdd.Acls) == 1 &&
		len(toRemove.Acls) == 1 &&
		cmp.Equal(toAdd.Acls[0], saramaACL1) &&
		cmp.Equal(toRemove.Acls[0], saramaACL2)) {
		t.Log("Failed test: A single new ACL should initiate an action to create it and delete the old ACL")
		t.Fail()
	}

	toAdd, toRemove = diffACLs(resourceACL4, resource1)
	if !(len(toAdd.Acls) == 1 &&
		len(toRemove.Acls) == 0) {
		t.Log("Failed test: Adding an ACL to the existing ACLs should register one object for creation and nothing for deletion")
		t.Fail()
	}

	toAdd, toRemove = diffACLs(resourceACL7, resourceACL6)

	if !(len(toAdd.Acls) == 1 && len(toRemove.Acls) == 0 &&
		toAdd.ResourceName == resourceACL6.ResourceName &&
		toAdd.ResourceType == resourceACL6.ResourceType &&
		toAdd.ResourcePatternType == resourceACL6.ResourcePatternType) {
		t.Log("Failed test: Adding an ACL to an existing resource (name, type, pattern) should create an ACL object accordingly")
		t.Fail()
	}
}

func TestValidateACLsFile(t *testing.T) {
	resourceACLs4 := sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTopic, ResourceName: "acls4"},
		Acls:     []*sarama.Acl{saramaACL3, saramaACL4}}
	kafkaACLs := saramaToKafkaResources([]sarama.ResourceAcls{resourceACLs4})

	res := ValidateACLs(kafkaACLs)
	if res {
		t.Log("Failed test: File with duplicate ACLs should be marked as invalid")
		t.Fail()
	}

	kafkaAclsValid := saramaToKafkaResources(resourcesACLs)
	res2 := ValidateACLs(kafkaAclsValid)
	if !res2 {
		t.Log("Failed test: Valid file should be marked as valid")
		t.Fail()
	}
}
