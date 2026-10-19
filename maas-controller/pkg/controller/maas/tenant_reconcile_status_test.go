package maas

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	maasv1alpha1 "github.com/opendatahub-io/models-as-a-service/maas-controller/api/maas/v1alpha1"
	"github.com/opendatahub-io/models-as-a-service/maas-controller/pkg/platform/tenantreconcile"
)

func TestSetFinalStatus_PraxisAvailabilityDoesNotBlockTenantActivation(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, maasv1alpha1.AddToScheme(scheme))
	tenant := &maasv1alpha1.MaasTenantConfig{ObjectMeta: metav1.ObjectMeta{
		Name: maasv1alpha1.MaasTenantConfigInstanceName, Namespace: "models-as-a-service",
	}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(tenant).WithObjects(tenant).Build()
	r := &TenantReconciler{Client: cl}

	result, err := r.setFinalStatus(ctx, tenant, &tenantreconcile.RunResult{PraxisUnavailable: "Praxis deployment not found"})
	require.NoError(t, err)
	assert.Equal(t, 20*time.Second, result.RequeueAfter)

	key := types.NamespacedName{Namespace: tenant.Namespace, Name: tenant.Name}
	got := &maasv1alpha1.MaasTenantConfig{}
	require.NoError(t, cl.Get(ctx, key, got))
	assert.Equal(t, "Active", got.Status.Phase)
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, tenantreconcile.ReadyConditionType))
	available := apimeta.FindStatusCondition(got.Status.Conditions, tenantreconcile.ConditionDeploymentsAvailable)
	require.NotNil(t, available)
	assert.Equal(t, metav1.ConditionFalse, available.Status)
	assert.Equal(t, "PayloadProcessingNotReady", available.Reason)

	result, err = r.setFinalStatus(ctx, got, &tenantreconcile.RunResult{})
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, result.RequeueAfter)
	require.NoError(t, cl.Get(ctx, key, got))
	assert.True(t, apimeta.IsStatusConditionTrue(got.Status.Conditions, tenantreconcile.ConditionDeploymentsAvailable))
}
