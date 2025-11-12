package franz

import (
	"testing"

	"github.com/IBM/sarama"
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

	// TransactionalID resource types for testing
	resourceACLTransactional1 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTransactionalID, ResourceName: "txn-producer-1"},
		Acls:     []*sarama.Acl{saramaACL1},
	}
	resourceACLTransactional2 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTransactionalID, ResourceName: "txn-producer-2", ResourcePatternType: sarama.AclPatternLiteral},
		Acls:     []*sarama.Acl{saramaACL2, saramaACL3},
	}
	resourceACLTransactional3 = sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTransactionalID, ResourceName: "txn-producer-*", ResourcePatternType: sarama.AclPatternPrefixed},
		Acls:     []*sarama.Acl{saramaACL1, saramaACL2},
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

// TestTransactionalIDConversion tests round-trip conversion of TransactionalID ACLs
func TestTransactionalIDConversion(t *testing.T) {
	resourcesWithTxn := []sarama.ResourceAcls{resourceACLTransactional1, resourceACLTransactional2}

	// convert sarama -> franz
	kafkaACLs := saramaToKafkaResources(resourcesWithTxn)

	// verify TransactionalID field is populated
	if len(kafkaACLs.TransactionalID) != 2 {
		t.Errorf("Failed test: Expected 2 TransactionalID ACLs, got %d", len(kafkaACLs.TransactionalID))
		t.Fail()
	}

	// convert franz -> sarama
	saramaResources := kafkaToSaramaResources(kafkaACLs)

	// verify round-trip conversion
	if !cmp.Equal(saramaResources, resourcesWithTxn) {
		t.Log("Failed test: TransactionalID round-trip conversion failed")
		t.Logf("Expected: %+v", resourcesWithTxn)
		t.Logf("Got: %+v", saramaResources)
		t.Fail()
	}
}

// TestMixedResourceTypesConversion tests conversion with all resource types including TransactionalID
func TestMixedResourceTypesConversion(t *testing.T) {
	mixedResources := []sarama.ResourceAcls{
		resourceACLs1,             // Cluster
		resourceACLs2,             // Group
		resourceACL3,              // Topic
		resourceACLTransactional1, // TransactionalID
	}

	// convert sarama -> franz
	kafkaACLs := saramaToKafkaResources(mixedResources)

	// verify all fields are populated
	if len(kafkaACLs.Cluster) != 1 {
		t.Errorf("Failed test: Expected 1 Cluster ACL, got %d", len(kafkaACLs.Cluster))
		t.Fail()
	}
	if len(kafkaACLs.ConsumerGroup) != 1 {
		t.Errorf("Failed test: Expected 1 ConsumerGroup ACL, got %d", len(kafkaACLs.ConsumerGroup))
		t.Fail()
	}
	if len(kafkaACLs.Topic) != 1 {
		t.Errorf("Failed test: Expected 1 Topic ACL, got %d", len(kafkaACLs.Topic))
		t.Fail()
	}
	if len(kafkaACLs.TransactionalID) != 1 {
		t.Errorf("Failed test: Expected 1 TransactionalID ACL, got %d", len(kafkaACLs.TransactionalID))
		t.Fail()
	}

	// convert franz -> sarama
	saramaResources := kafkaToSaramaResources(kafkaACLs)

	// verify round-trip conversion
	if !cmp.Equal(saramaResources, mixedResources) {
		t.Log("Failed test: Mixed resource types round-trip conversion failed")
		t.Fail()
	}
}

// TestDiffWithTransactionalID tests diff calculation with TransactionalID resources
func TestDiffWithTransactionalID(t *testing.T) {
	// test adding a new TransactionalID resource
	existingResources := []sarama.ResourceAcls{resourceACLs1, resourceACLs2}
	newResources := []sarama.ResourceAcls{resourceACLs1, resourceACLs2, resourceACLTransactional1}

	toCreate, toDelete := diffResourcesACLs(newResources, existingResources)

	if len(toCreate) != 1 || len(toDelete) != 0 {
		t.Errorf("Failed test: Adding TransactionalID should create 1 resource, got create=%d, delete=%d", len(toCreate), len(toDelete))
		t.Fail()
	}

	if toCreate[0].ResourceType != sarama.AclResourceTransactionalID {
		t.Log("Failed test: Created resource should be of type TransactionalID")
		t.Fail()
	}

	// test removing a TransactionalID resource
	toCreate, toDelete = diffResourcesACLs(existingResources, newResources)

	if len(toCreate) != 0 || len(toDelete) != 1 {
		t.Errorf("Failed test: Removing TransactionalID should delete 1 resource, got create=%d, delete=%d", len(toCreate), len(toDelete))
		t.Fail()
	}

	if toDelete[0].ResourceType != sarama.AclResourceTransactionalID {
		t.Log("Failed test: Deleted resource should be of type TransactionalID")
		t.Fail()
	}
}

// TestTransactionalIDPatternTypes tests TransactionalID with different pattern types
func TestTransactionalIDPatternTypes(t *testing.T) {
	// test with Literal pattern type
	resourcesLiteral := []sarama.ResourceAcls{resourceACLTransactional2}
	kafkaACLs := saramaToKafkaResources(resourcesLiteral)

	if len(kafkaACLs.TransactionalID) != 1 {
		t.Errorf("Failed test: Expected 1 TransactionalID ACL with Literal pattern, got %d", len(kafkaACLs.TransactionalID))
		t.Fail()
	}

	if kafkaACLs.TransactionalID[0].PatternType != "Literal" {
		t.Errorf("Failed test: Expected pattern type 'Literal', got '%s'", kafkaACLs.TransactionalID[0].PatternType)
		t.Fail()
	}

	// test with Prefixed pattern type
	resourcesPrefixed := []sarama.ResourceAcls{resourceACLTransactional3}
	kafkaACLs2 := saramaToKafkaResources(resourcesPrefixed)

	if len(kafkaACLs2.TransactionalID) != 1 {
		t.Errorf("Failed test: Expected 1 TransactionalID ACL with Prefixed pattern, got %d", len(kafkaACLs2.TransactionalID))
		t.Fail()
	}

	if kafkaACLs2.TransactionalID[0].PatternType != "Prefixed" {
		t.Errorf("Failed test: Expected pattern type 'Prefixed', got '%s'", kafkaACLs2.TransactionalID[0].PatternType)
		t.Fail()
	}

	// verify round-trip conversion preserves pattern types
	saramaResources := kafkaToSaramaResources(kafkaACLs)
	if !cmp.Equal(saramaResources, resourcesLiteral) {
		t.Log("Failed test: Literal pattern type round-trip conversion failed")
		t.Fail()
	}

	saramaResources2 := kafkaToSaramaResources(kafkaACLs2)
	if !cmp.Equal(saramaResources2, resourcesPrefixed) {
		t.Log("Failed test: Prefixed pattern type round-trip conversion failed")
		t.Fail()
	}
}

// TestValidateTransactionalIDACLs tests validation with TransactionalID ACLs
func TestValidateTransactionalIDACLs(t *testing.T) {
	// test with valid TransactionalID ACLs
	validResources := []sarama.ResourceAcls{resourceACLTransactional1, resourceACLTransactional2}
	kafkaACLs := saramaToKafkaResources(validResources)

	if !ValidateACLs(kafkaACLs) {
		t.Log("Failed test: Valid TransactionalID ACLs should be marked as valid")
		t.Fail()
	}

	// test with duplicate TransactionalID ACLs
	saramaACLDup := &sarama.Acl{
		Principal:      "principal1",
		Host:           "host1",
		Operation:      sarama.AclOperationWrite,
		PermissionType: sarama.AclPermissionAllow,
	}
	resourceACLDup := sarama.ResourceAcls{
		Resource: sarama.Resource{ResourceType: sarama.AclResourceTransactionalID, ResourceName: "txn-dup"},
		Acls:     []*sarama.Acl{saramaACLDup, saramaACLDup},
	}
	kafkaACLsDup := saramaToKafkaResources([]sarama.ResourceAcls{resourceACLDup})

	if ValidateACLs(kafkaACLsDup) {
		t.Log("Failed test: Duplicate TransactionalID ACLs should be marked as invalid")
		t.Fail()
	}
}
