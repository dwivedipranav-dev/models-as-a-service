package tenantreconcile

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	goruntime "runtime"
	"slices"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	maasv1alpha1 "github.com/opendatahub-io/models-as-a-service/maas-controller/api/maas/v1alpha1"
)

type appliedResource struct {
	gvk       schema.GroupVersionKind
	namespace string
	name      string
}

func praxisTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(maasv1alpha1.AddToScheme(scheme))
	utilruntime.Must(gwapiv1.Install(scheme))
	return scheme
}

// newSSAFakeClientBuilder returns a fake client builder that can server-side apply NetworkPolicies.
// controller-runtime's fake client (v0.23+) lists NetworkPolicy as having a status
// subresource, which the built-in apply schema does not have, so applying one fails with
// "expected objects with types from the same schema". The deduced type converter avoids it.
func newSSAFakeClientBuilder(scheme *runtime.Scheme) *fake.ClientBuilder {
	return fake.NewClientBuilder().WithScheme(scheme).WithTypeConverters(managedfields.NewDeducedTypeConverter())
}

func platformOverlayManifestPath(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := goruntime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(
		filepath.Dir(currentFile),
		"..", "..", "..", "..",
		"maas-api", "deploy", "overlays", "odh",
	))
}

func readyMaaSAPIDeployment(namespace, tenantID string) *appsv1.Deployment {
	replicas := int32(1)
	name := MaaSAPIDeploymentName(tenantID)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "maas-api", Image: "test"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			UpdatedReplicas:    1,
			AvailableReplicas:  1,
			ReadyReplicas:      1,
			Replicas:           1,
		},
	}
}

func praxisTenantConfig(namespace, tenantName string) *maasv1alpha1.MaasTenantConfig {
	return &maasv1alpha1.MaasTenantConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      maasv1alpha1.MaasTenantConfigInstanceName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelManagedByAITenant: "true",
				LabelTenantName:        tenantName,
				LabelTenantNamespace:   namespace,
			},
			Annotations: map[string]string{
				AnnotationAITenantName:      tenantName,
				AnnotationAITenantNamespace: DefaultAITenantNamespace,
			},
		},
	}
}

func runPlatformTestClient(
	t *testing.T,
	scheme *runtime.Scheme,
	seed []client.Object,
	recordApplied *[]appliedResource,
) client.Client {
	t.Helper()
	builder := newSSAFakeClientBuilder(scheme).WithObjects(seed...)
	if recordApplied != nil {
		builder = builder.WithInterceptorFuncs(interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				// client.ApplyConfigurationFromUnstructured wraps the Unstructured object.
				if u, ok := obj.(interface {
					GroupVersionKind() schema.GroupVersionKind
					GetNamespace() string
					GetName() string
				}); ok {
					*recordApplied = append(*recordApplied, appliedResource{
						gvk:       u.GroupVersionKind(),
						namespace: u.GetNamespace(),
						name:      u.GetName(),
					})
				}
				return c.Apply(ctx, obj, opts...)
			},
		})
	}
	return builder.Build()
}

func TestRunPlatform_PraxisSkipsIPPApply(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	var applied []appliedResource
	cl := runPlatformTestClient(t, scheme, []client.Object{
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName),
	}, &applied)

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.DeploymentPending, "praxis tenant should not wait on IPP EnvoyFilter: %s", result.Detail)
	assert.Contains(t, result.PraxisUnavailable, "Praxis deployment")

	hasMaaSAPIDeployment := false
	for _, res := range applied {
		if res.gvk == GVKDeployment && strings.HasPrefix(res.name, baseMaaSAPIDeploymentName) {
			hasMaaSAPIDeployment = true
		}
		if isIPPResource(res.gvk, res.name) {
			t.Fatalf("unexpected IPP resource applied for praxis tenant: %s %s/%s", res.gvk.String(), res.namespace, res.name)
		}
	}
	assert.True(t, hasMaaSAPIDeployment, "expected maas-api Deployment to be applied")
}

func readyPraxisDeployment(namespace, name, configPath string) *appsv1.Deployment {
	dep := readyMaaSAPIDeployment(namespace, "")
	dep.Name = name
	dep.Spec.Template.Spec.Containers[0].Image = "example/odh-praxis-extproc:test"
	dep.Spec.Template.Spec.Containers[0].Args = []string{"-c", configPath}
	return dep
}

func readyPraxisControllerDeployment() *appsv1.Deployment {
	dep := readyMaaSAPIDeployment("controller-ns", "")
	dep.Name = "ai-gateway-controller"
	dep.Spec.Selector.MatchLabels["app"] = dep.Name
	dep.Spec.Template.Labels["app"] = dep.Name
	dep.Spec.Template.Spec.Containers[0].Name = dep.Name
	dep.Spec.Template.Spec.ServiceAccountName = "ai-gateway-controller"
	return dep
}

func gatewayAccessReviews(t *testing.T, deniedVerb string) interceptor.Funcs {
	t.Helper()
	return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if review, ok := obj.(*authv1.SubjectAccessReview); ok {
			require.Equal(t, "system:serviceaccount:controller-ns:ai-gateway-controller", review.Spec.User)
			assert.ElementsMatch(t, []string{
				"system:serviceaccounts", "system:serviceaccounts:controller-ns", "system:authenticated",
			}, review.Spec.Groups)
			attributes := review.Spec.ResourceAttributes
			require.NotNil(t, attributes)
			assert.Equal(t, "gateway.networking.k8s.io", attributes.Group)
			assert.Equal(t, "gateways", attributes.Resource)
			if attributes.Verb == "get" {
				assert.Equal(t, "openshift-ingress", attributes.Namespace)
				assert.Equal(t, "praxis-gateway", attributes.Name)
			} else {
				assert.Empty(t, attributes.Namespace)
				assert.Empty(t, attributes.Name)
			}
			review.Status.Allowed = attributes.Verb != deniedVerb
			return nil
		}
		return c.Create(ctx, obj, opts...)
	}}
}

func TestPraxisControllerCanReadGateway_GroupBasedAccess(t *testing.T) {
	const namespaceGroup = "system:serviceaccounts:controller-ns"
	var reviewedVerbs []string
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(
		readyPraxisControllerDeployment(),
	).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			review, ok := obj.(*authv1.SubjectAccessReview)
			if !ok {
				return c.Create(ctx, obj, opts...)
			}
			require.Equal(t, "system:serviceaccount:controller-ns:ai-gateway-controller", review.Spec.User)
			assert.ElementsMatch(t, []string{
				"system:serviceaccounts", namespaceGroup, "system:authenticated",
			}, review.Spec.Groups)
			require.NotNil(t, review.Spec.ResourceAttributes)
			reviewedVerbs = append(reviewedVerbs, review.Spec.ResourceAttributes.Verb)
			// Model a Group binding: the named service account has no direct grant.
			review.Status.Allowed = slices.Contains(review.Spec.Groups, namespaceGroup)
			return nil
		},
	}).Build()

	ready, detail, err := praxisControllerCanReadGateway(context.Background(), cl, "controller-ns", "openshift-ingress", "praxis-gateway")
	require.NoError(t, err)
	assert.True(t, ready, detail)
	assert.Empty(t, detail)
	assert.Equal(t, []string{"get", "list", "watch"}, reviewedVerbs)
}

func TestPraxisControllerCanReadGateway_IgnoresOtherNamespaces(t *testing.T) {
	unrelated := readyPraxisControllerDeployment()
	unrelated.Namespace = "unrelated-ns"
	unrelated.Spec.Template.Spec.ServiceAccountName = "unrelated-service-account"
	for _, tt := range []struct {
		name        string
		includePeer bool
	}{
		{name: "only unrelated deployment"},
		{name: "peer and unrelated deployment", includePeer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objects := []client.Object{unrelated}
			if tt.includePeer {
				objects = append(objects, readyPraxisControllerDeployment())
			}
			reviews := 0
			cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(objects...).
				WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if review, ok := obj.(*authv1.SubjectAccessReview); ok {
						reviews++
						assert.Equal(t, "system:serviceaccount:controller-ns:ai-gateway-controller", review.Spec.User)
						review.Status.Allowed = true
						return nil
					}
					return c.Create(ctx, obj, opts...)
				}}).Build()

			ready, detail, err := praxisControllerCanReadGateway(context.Background(), cl, "controller-ns", "openshift-ingress", "praxis-gateway")
			require.NoError(t, err)
			if tt.includePeer {
				assert.True(t, ready, detail)
				assert.Empty(t, detail)
				assert.Equal(t, 3, reviews)
			} else {
				assert.False(t, ready)
				assert.Contains(t, detail, "deployment not found")
				assert.Zero(t, reviews)
			}
		})
	}
}

func deepCopyPatch(t *testing.T, patch map[string]any) map[string]any {
	t.Helper()
	out, ok := runtime.DeepCopyJSONValue(patch).(map[string]any)
	require.True(t, ok)
	return out
}

func TestPraxisConfigPath(t *testing.T) {
	const path = "/etc/praxis/extproc.yaml"
	for _, tt := range []struct {
		name string
		argv []string
		want string
	}{
		{name: "Praxis short option", argv: []string{"praxis-extproc", "-c", path}, want: path},
		{name: "separate value", argv: []string{"praxis", "--config", path}, want: path},
		{name: "equals value", argv: []string{"praxis", "--config=" + path}, want: path},
		{name: "suffixed value", argv: []string{"--config=" + path + ".backup"}, want: path + ".backup"},
		{name: "suffixed short-option value", argv: []string{"-c", path + ".backup"}, want: path + ".backup"},
		{name: "bare path", argv: []string{path}},
		{name: "different option", argv: []string{"--other-config=" + path}},
		{name: "final option wins", argv: []string{"--config", path, "--config=" + path + ".backup"}, want: path + ".backup"},
		{name: "final short option wins", argv: []string{"--config", path, "-c", path + ".backup"}, want: path + ".backup"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, praxisConfigPath(tt.argv))
		})
	}
}

func TestPraxisPayloadProcessingReady(t *testing.T) {
	const (
		gwNS     = "openshift-ingress"
		gwName   = "team-gateway"
		tenantID = "team"
	)
	processing := readyPraxisDeployment(gwNS, PayloadProcessingDeploymentName(tenantID), "/etc/praxis/extproc.yaml")
	preProcessing := readyPraxisDeployment(gwNS, PayloadPreProcessingDeploymentName(tenantID), "/etc/praxis/pre-extproc.yaml")
	processingService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: PayloadProcessingServiceName(tenantID), Namespace: gwNS}}
	preProcessingService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: PayloadPreProcessingServiceName(tenantID), Namespace: gwNS}}
	ef := payloadProcessingEnvoyFilter(gwNS, PayloadProcessingEnvoyFilterName(tenantID), gwName, nil)
	processingPatch := map[string]any{
		"applyTo": "HTTP_FILTER",
		"patch": map[string]any{
			"operation": "INSERT_AFTER",
			"value": map[string]any{
				"name": "envoy.filters.http.ext_proc.ipp",
				"typed_config": map[string]any{
					"@type": "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor",
					"grpc_service": map[string]any{
						"envoy_grpc": map[string]any{"cluster_name": "outbound|9004||payload-processing-team.openshift-ingress.svc.cluster.local"},
					},
				},
			},
		},
	}
	preProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(preProcessingPatch, "envoy.filters.http.ext_proc.ipp-pre", "patch", "value", "name"))
	require.NoError(t, unstructured.SetNestedField(preProcessingPatch, "outbound|9004||payload-pre-processing-team.openshift-ingress.svc.cluster.local",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	require.NoError(t, unstructured.SetNestedSlice(ef.Object, []any{preProcessingPatch, processingPatch}, "spec", "configPatches"))
	missingPatch := payloadProcessingEnvoyFilter(gwNS, PayloadProcessingEnvoyFilterName(tenantID), gwName, nil)
	missingPrePatch := ef.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(missingPrePatch.Object, []any{processingPatch}, "spec", "configPatches"))
	missingProcessingPatch := ef.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(missingProcessingPatch.Object, []any{preProcessingPatch}, "spec", "configPatches"))
	wrongPatch := ef.DeepCopy()
	wrongProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(wrongProcessingPatch, "other-service.openshift-ingress.svc.cluster.local",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	require.NoError(t, unstructured.SetNestedSlice(wrongPatch.Object, []any{preProcessingPatch, wrongProcessingPatch}, "spec", "configPatches"))
	sidecarProcessingEF := ef.DeepCopy()
	sidecarProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(sidecarProcessingPatch, "SIDECAR_OUTBOUND", "match", "context"))
	require.NoError(t, unstructured.SetNestedSlice(sidecarProcessingEF.Object,
		[]any{preProcessingPatch, sidecarProcessingPatch}, "spec", "configPatches"))
	wrongServicePortPatch := ef.DeepCopy()
	wrongServicePortProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(wrongServicePortProcessingPatch,
		"outbound|9005||payload-processing-team.openshift-ingress.svc.cluster.local",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	require.NoError(t, unstructured.SetNestedSlice(wrongServicePortPatch.Object,
		[]any{preProcessingPatch, wrongServicePortProcessingPatch}, "spec", "configPatches"))
	prefixedServicePatch := ef.DeepCopy()
	prefixedServiceProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(prefixedServiceProcessingPatch,
		"outbound|9004||evil-payload-processing-team.openshift-ingress.svc.cluster.local",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	require.NoError(t, unstructured.SetNestedSlice(prefixedServicePatch.Object,
		[]any{preProcessingPatch, prefixedServiceProcessingPatch}, "spec", "configPatches"))
	fixedClusterNoAddEF := ef.DeepCopy()
	fixedPreProcessingPatch := deepCopyPatch(t, preProcessingPatch)
	require.NoError(t, unstructured.SetNestedField(fixedPreProcessingPatch, "payload-pre-processing-extproc",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	fixedProcessingPatch := deepCopyPatch(t, processingPatch)
	require.NoError(t, unstructured.SetNestedField(fixedProcessingPatch, "payload-processing-extproc",
		"patch", "value", "typed_config", "grpc_service", "envoy_grpc", "cluster_name"))
	require.NoError(t, unstructured.SetNestedSlice(fixedClusterNoAddEF.Object, []any{fixedPreProcessingPatch, fixedProcessingPatch}, "spec", "configPatches"))
	clusterAddPatch := func(name, serviceName string, port int64) map[string]any {
		socketAddress := map[string]any{
			"address":    serviceName + "." + gwNS + ".svc.cluster.local",
			"port_value": port,
		}
		endpoint := map[string]any{"address": map[string]any{"socket_address": socketAddress}}
		assignment := map[string]any{
			"cluster_name": name,
			"endpoints": []any{map[string]any{
				"lb_endpoints": []any{map[string]any{"endpoint": endpoint}},
			}},
		}
		return map[string]any{
			"applyTo": "CLUSTER",
			"match":   map[string]any{"context": "GATEWAY"},
			"patch": map[string]any{
				"operation": "ADD",
				"value": map[string]any{
					"name":            name,
					"type":            "STRICT_DNS",
					"load_assignment": assignment,
				},
			},
		}
	}
	preClusterAdd := clusterAddPatch("payload-pre-processing-extproc", PayloadPreProcessingServiceName(tenantID), 9004)
	processingClusterAdd := clusterAddPatch("payload-processing-extproc", PayloadProcessingServiceName(tenantID), 9004)
	fixedClusterEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(fixedClusterEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch, preClusterAdd, processingClusterAdd}, "spec", "configPatches"))
	missingPreClusterEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(missingPreClusterEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch, processingClusterAdd}, "spec", "configPatches"))
	missingProcessingClusterEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(missingProcessingClusterEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch, preClusterAdd}, "spec", "configPatches"))
	missingEndpointAdd := deepCopyPatch(t, preClusterAdd)
	unstructured.RemoveNestedField(missingEndpointAdd, "patch", "value", "load_assignment")
	missingEndpointEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(missingEndpointEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch, missingEndpointAdd, processingClusterAdd}, "spec", "configPatches"))
	wrongServiceEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(wrongServiceEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch,
			clusterAddPatch("payload-pre-processing-extproc", PayloadProcessingServiceName(tenantID), 9004), processingClusterAdd},
		"spec", "configPatches"))
	wrongPortEF := fixedClusterNoAddEF.DeepCopy()
	require.NoError(t, unstructured.SetNestedSlice(wrongPortEF.Object,
		[]any{fixedPreProcessingPatch, fixedProcessingPatch, preClusterAdd,
			clusterAddPatch("payload-processing-extproc", PayloadProcessingServiceName(tenantID), 9005)},
		"spec", "configPatches"))
	mirroredPreProcessing := preProcessing.DeepCopy()
	mirroredPreProcessing.Spec.Template.Spec.Containers[0].Image = "registry.example/custom:latest"
	mirroredPreProcessing.Spec.Template.Spec.Containers[0].Args = nil
	mirroredPreProcessing.Spec.Template.Spec.Containers[0].Command = []string{"praxis", "--config=/etc/praxis/pre-extproc.yaml"}
	mirroredProcessing := processing.DeepCopy()
	mirroredProcessing.Spec.Template.Spec.Containers[0].Image = "registry.example/custom:latest"
	mirroredProcessing.Spec.Template.Spec.Containers[0].Args = []string{"--config", "/etc/praxis/extproc.yaml"}
	wrongPreProcessing := mirroredPreProcessing.DeepCopy()
	wrongPreProcessing.Spec.Template.Spec.Containers[0].Command = []string{"praxis", "--config=/etc/praxis/extproc.yaml"}
	wrongPraxisPreProcessing := preProcessing.DeepCopy()
	wrongPraxisPreProcessing.Spec.Template.Spec.Containers[0].Args = []string{"--config", "/etc/praxis/extproc.yaml"}
	wrongPraxisProcessing := processing.DeepCopy()
	wrongPraxisProcessing.Spec.Template.Spec.Containers[0].Args = []string{"--config", "/etc/praxis/pre-extproc.yaml"}
	backupProcessing := processing.DeepCopy()
	backupProcessing.Spec.Template.Spec.Containers[0].Args = []string{"--config=/etc/praxis/extproc.yaml.backup"}
	barePathProcessing := processing.DeepCopy()
	barePathProcessing.Spec.Template.Spec.Containers[0].Args = []string{"/etc/praxis/extproc.yaml"}

	tests := []struct {
		name   string
		seed   []client.Object
		detail string
		ready  bool
	}{
		{name: "missing deployment", detail: "Praxis deployment"},
		{name: "legacy deployment", seed: []client.Object{func() client.Object {
			dep := preProcessing.DeepCopy()
			dep.Spec.Template.Spec.Containers[0].Image = "example/legacy-ipp:test"
			dep.Spec.Template.Spec.Containers[0].Args = nil
			return dep
		}()}, detail: "does not use Praxis config"},
		{name: "unavailable deployment", seed: []client.Object{func() client.Object {
			dep := preProcessing.DeepCopy()
			dep.Status.AvailableReplicas = 0
			return dep
		}()}, detail: "not available"},
		{name: "wrong mirrored pre-processing config", seed: []client.Object{wrongPreProcessing}, detail: "does not use Praxis config"},
		{name: "wrong normal-image pre-processing config", seed: []client.Object{wrongPraxisPreProcessing}, detail: "does not use Praxis config"},
		{name: "wrong normal-image processing config", seed: []client.Object{preProcessing, wrongPraxisProcessing}, detail: "does not use Praxis config"},
		{name: "backup processing config", seed: []client.Object{preProcessing, backupProcessing}, detail: "does not use Praxis config"},
		{name: "path without config option", seed: []client.Object{preProcessing, barePathProcessing}, detail: "does not use Praxis config"},
		{name: "missing service", seed: []client.Object{preProcessing, processing}, detail: "Praxis Service"},
		{name: "missing EnvoyFilter", seed: []client.Object{preProcessing, processing, preProcessingService, processingService}, detail: "Praxis EnvoyFilter"},
		{name: "wrong gateway", seed: []client.Object{preProcessing, processing, preProcessingService, processingService,
			payloadProcessingEnvoyFilter(gwNS, PayloadProcessingEnvoyFilterName(tenantID), "other-gateway", nil)}, detail: "does not select gateway"},
		{name: "missing config patches", seed: []client.Object{preProcessing, processing, preProcessingService, processingService, missingPatch}, detail: "has no pre-processing HTTP_FILTER patch"},
		{name: "missing pre-processing patch", seed: []client.Object{preProcessing, processing, preProcessingService, processingService, missingPrePatch}, detail: "has no pre-processing HTTP_FILTER patch"},
		{name: "missing processing patch", seed: []client.Object{preProcessing, processing, preProcessingService, processingService, missingProcessingPatch}, detail: "has no processing HTTP_FILTER patch"},
		{name: "wrong processing patch cluster", seed: []client.Object{preProcessing, processing, preProcessingService, processingService, wrongPatch}, detail: "has no processing HTTP_FILTER patch"},
		{name: "processing patch matches sidecars only", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, sidecarProcessingEF}, detail: "has no processing HTTP_FILTER patch"},
		{name: "processing patch cluster uses wrong port", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, wrongServicePortPatch}, detail: "has no processing HTTP_FILTER patch"},
		{name: "processing patch cluster uses prefixed service", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, prefixedServicePatch}, detail: "has no processing HTTP_FILTER patch"},
		{name: "fixed clusters without ADD patches", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, fixedClusterNoAddEF}, detail: "matching CLUSTER ADD patch"},
		{name: "fixed pre-processing cluster without ADD patch", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, missingPreClusterEF}, detail: "matching CLUSTER ADD patch"},
		{name: "fixed processing cluster without ADD patch", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, missingProcessingClusterEF}, detail: "matching CLUSTER ADD patch"},
		{name: "fixed pre-processing cluster without endpoint", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, missingEndpointEF}, detail: "matching CLUSTER ADD patch"},
		{name: "fixed pre-processing cluster points to wrong service", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, wrongServiceEF}, detail: "matching CLUSTER ADD patch"},
		{name: "fixed processing cluster uses wrong port", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, wrongPortEF}, detail: "matching CLUSTER ADD patch"},
		{name: "ready", seed: []client.Object{preProcessing, processing, preProcessingService, processingService, ef}, ready: true},
		{name: "ready with fixed Praxis cluster names", seed: []client.Object{preProcessing, processing,
			preProcessingService, processingService, fixedClusterEF}, ready: true},
		{name: "ready with mirrored images", seed: []client.Object{mirroredPreProcessing, mirroredProcessing,
			preProcessingService, processingService, ef}, ready: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(tt.seed...).Build()
			ready, detail, err := PraxisPayloadProcessingReady(context.Background(), cl, gwNS, gwName, tenantID)
			require.NoError(t, err)
			assert.Equal(t, tt.ready, ready)
			assert.Contains(t, detail, tt.detail)
		})
	}
}

func TestRunPlatform_PraxisRetainsExistingIPPWithoutPeer(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	ippDeployment := unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantName), nil)
	setConfigControllerOwnerRef(ippDeployment, mcfg.UID)
	ippEnvoyFilter := unstructuredIPPObject(GVKEnvoyFilter, gwNS, PayloadProcessingEnvoyFilterName(tenantName), nil)
	setConfigControllerOwnerRef(ippEnvoyFilter, mcfg.UID)
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	cl := newSSAFakeClientBuilder(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), ippDeployment, ippEnvoyFilter,
	).Build()

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DeploymentPending)
	assert.Contains(t, result.Detail, "ai-gateway-controller deployment not found")

	depKey := types.NamespacedName{
		Namespace: gwNS,
		Name:      PayloadProcessingDeploymentName(tenantName),
	}
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(GVKDeployment)
	require.NoError(t, cl.Get(context.Background(), depKey, dep))

	efKey := types.NamespacedName{
		Namespace: gwNS,
		Name:      PayloadProcessingEnvoyFilterName(tenantName),
	}
	ef := &unstructured.Unstructured{}
	ef.SetGroupVersionKind(GVKEnvoyFilter)
	require.NoError(t, cl.Get(context.Background(), efKey, ef))

	gotTenant := &maasv1alpha1.MaasTenantConfig{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: appNs, Name: maasv1alpha1.MaasTenantConfigInstanceName}, gotTenant))
	assert.Empty(t, gotTenant.Annotations[AnnotationPayloadProcessingStatus])
}

func TestReconcilePayloadProcessingSwitch_GatewayAccessPreflight(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	for _, tt := range []struct {
		name       string
		deniedVerb string
		cleanup    bool
	}{
		{name: "get denied", deniedVerb: "get"},
		{name: "list denied", deniedVerb: "list"},
		{name: "watch denied", deniedVerb: "watch"},
		{name: "Gateway access granted", cleanup: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := praxisTestScheme(t)
			tenant := praxisTenantConfig(appNs, tenantName)
			mcfg := &maasv1alpha1.Config{ObjectMeta: metav1.ObjectMeta{
				Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid"),
			}}
			ippDeployment := unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantName), nil)
			setConfigControllerOwnerRef(ippDeployment, mcfg.UID)
			ippEnvoyFilter := unstructuredIPPObject(GVKEnvoyFilter, gwNS, PayloadProcessingEnvoyFilterName(tenantName), nil)
			setConfigControllerOwnerRef(ippEnvoyFilter, mcfg.UID)
			cl := newSSAFakeClientBuilder(scheme).WithObjects(
				tenant, readyPraxisControllerDeployment(), ippDeployment, ippEnvoyFilter,
			).WithInterceptorFuncs(gatewayAccessReviews(t, tt.deniedVerb)).Build()
			params := PlatformParams{
				SkipIPP: true, GatewayNamespace: gwNS, GatewayName: gwName,
				ModelNamespace: appNs, TenantIdentifier: tenantName, ControllerNamespace: "controller-ns",
			}
			outcome, err := reconcilePayloadProcessingSwitch(ctx, cl, tenant, &params, mcfg, logr.Discard())
			require.NoError(t, err)
			if tt.cleanup {
				assert.Empty(t, outcome.blockedReason)
			} else {
				assert.Contains(t, outcome.blockedReason, "cannot "+tt.deniedVerb+" Gateways")
			}
			for _, ref := range []ippResourceRef{
				{gvk: GVKDeployment, namespace: gwNS, name: PayloadProcessingDeploymentName(tenantName)},
				{gvk: GVKEnvoyFilter, namespace: gwNS, name: PayloadProcessingEnvoyFilterName(tenantName)},
			} {
				obj := &unstructured.Unstructured{}
				obj.SetGroupVersionKind(ref.gvk)
				err := cl.Get(ctx, client.ObjectKey{Namespace: ref.namespace, Name: ref.name}, obj)
				if tt.cleanup {
					assert.True(t, apierrors.IsNotFound(err), "%s should be removed after access preflight", ref.gvk.Kind)
				} else {
					assert.NoError(t, err, "%s must remain while Gateway access is denied", ref.gvk.Kind)
				}
			}
			gotTenant := &maasv1alpha1.MaasTenantConfig{}
			require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(tenant), gotTenant))
			if tt.cleanup {
				assert.Equal(t, PayloadProcessingStatusCleanupComplete, payloadProcessingStatus(gotTenant))
			} else {
				assert.Empty(t, payloadProcessingStatus(gotTenant))
			}
		})
	}
}

func TestHasMaaSOwnedIPPDeployment(t *testing.T) {
	const gatewayNamespace = "openshift-ingress"
	params := PlatformParams{GatewayNamespace: gatewayNamespace, TenantIdentifier: "team"}
	configUID := types.UID("cfg-uid")
	preProcessing := unstructuredIPPObject(GVKDeployment, gatewayNamespace, PayloadPreProcessingDeploymentName("team"), nil)
	setConfigControllerOwnerRef(preProcessing, configUID)
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(preProcessing).Build()
	present, err := hasMaaSOwnedIPPDeployment(context.Background(), cl, params, configUID)
	require.NoError(t, err)
	assert.True(t, present, "pre-processing IPP alone must prevent premature cleanup")

	peerOwned := praxisOwnedDeployment(gatewayNamespace, PayloadProcessingDeploymentName("team"))
	cl = fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(peerOwned).Build()
	present, err = hasMaaSOwnedIPPDeployment(context.Background(), cl, params, configUID)
	require.NoError(t, err)
	assert.False(t, present, "Praxis-owned Deployment must not block stale IPP cleanup")
}

func TestRunPlatform_PraxisBlocksCleanupForNonStandardPluginsConfigMap(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	ippDeployment := unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantName), nil)
	setConfigControllerOwnerRef(ippDeployment, mcfg.UID)
	pluginsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PayloadProcessingPluginsConfigMapForTenant(tenantName),
			Namespace: gwNS,
		},
		Data: map[string]string{
			ippPreProcessingConfigKey: defaultPreProcessingYAML,
			ippProcessingConfigKey: `apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- type: custom-only
profiles:
- name: default
  plugins:
    request:
    - pluginRef: custom-only
    response: []
`,
		},
	}
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	cl := newSSAFakeClientBuilder(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), ippDeployment, pluginsCM,
	).Build()

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DeploymentPending)
	assert.Contains(t, result.Detail, AnnotationForcePayloadProcessingMigration)
	require.NotEmpty(t, result.Warnings)
	assert.Contains(t, result.Warnings[0], AnnotationForcePayloadProcessingMigration)

	// IPP Deployment must remain until force annotation is applied.
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(GVKDeployment)
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: gwNS,
		Name:      PayloadProcessingDeploymentName(tenantName),
	}, dep))

	// Non-IPP apply still runs while migration is blocked (maas-api stays ready).
	apiDep := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: appNs,
		Name:      MaaSAPIDeploymentName(tenantName),
	}, apiDep))

	gotTenant := &maasv1alpha1.MaasTenantConfig{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: appNs,
		Name:      maasv1alpha1.MaasTenantConfigInstanceName,
	}, gotTenant))
	assert.Equal(t, PayloadProcessingMigrationBlocked, gotTenant.Annotations[AnnotationPayloadProcessingMigration])
	assert.Empty(t, gotTenant.Annotations[AnnotationPayloadProcessingStatus])
}

func TestRunPlatform_PraxisForceAnnotationUnblocksCleanup(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	// Simulate the reconcile after an operator applies the force annotation on a
	// previously blocked tenant: blocked marker remains until cleanup succeeds.
	tenant.Annotations[AnnotationPayloadProcessingMigration] = PayloadProcessingMigrationBlocked
	tenant.Annotations[AnnotationForcePayloadProcessingMigration] = "true"
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	ippDeployment := unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantName), nil)
	setConfigControllerOwnerRef(ippDeployment, mcfg.UID)
	pluginsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PayloadProcessingPluginsConfigMapForTenant(tenantName),
			Namespace: gwNS,
		},
		Data: map[string]string{
			ippPreProcessingConfigKey: defaultPreProcessingYAML,
			ippProcessingConfigKey: `apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- type: custom-only
profiles:
- name: default
  plugins:
    request:
    - pluginRef: custom-only
    response: []
`,
		},
	}
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	cl := newSSAFakeClientBuilder(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), ippDeployment, pluginsCM,
		readyPraxisControllerDeployment(),
	).WithInterceptorFuncs(gatewayAccessReviews(t, "")).Build()

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(GVKDeployment)
	err = cl.Get(context.Background(), types.NamespacedName{
		Namespace: gwNS,
		Name:      PayloadProcessingDeploymentName(tenantName),
	}, dep)
	require.True(t, apierrors.IsNotFound(err), "force annotation should allow IPP cleanup")

	gotTenant := &maasv1alpha1.MaasTenantConfig{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: appNs,
		Name:      maasv1alpha1.MaasTenantConfigInstanceName,
	}, gotTenant))
	assert.Equal(t, PayloadProcessingStatusCleanupComplete, gotTenant.Annotations[AnnotationPayloadProcessingStatus])
	assert.Empty(t, gotTenant.Annotations[AnnotationPayloadProcessingMigration],
		"blocked migration annotation is cleared after successful cleanup")
	// Force annotation is operator-managed and is not cleared by the controller.
	assert.Equal(t, "true", gotTenant.Annotations[AnnotationForcePayloadProcessingMigration])
}

func TestRunPlatform_PraxisMigrationCleanupSkipsPraxisOwnedResources(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	praxisDeployment := praxisOwnedDeployment(gwNS, PayloadProcessingDeploymentName(tenantName))
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	cl := newSSAFakeClientBuilder(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), praxisDeployment,
	).Build()

	_, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)

	depKey := types.NamespacedName{Namespace: gwNS, Name: PayloadProcessingDeploymentName(tenantName)}
	got := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), depKey, got), "praxis-owned deployment should survive migration cleanup")
}

func TestRunPlatform_PraxisSkipsCleanupAfterMigrationComplete(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	tenant.Annotations[AnnotationPayloadProcessingStatus] = PayloadProcessingStatusCleanupComplete
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	legacyDeployment := unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantName), nil)
	setConfigControllerOwnerRef(legacyDeployment, mcfg.UID)
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		SkipIPP:    true,
		Source:     "aitenant",
	}

	cl := newSSAFakeClientBuilder(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), legacyDeployment,
	).Build()

	_, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)

	depKey := types.NamespacedName{Namespace: gwNS, Name: PayloadProcessingDeploymentName(tenantName)}
	dep := &unstructured.Unstructured{}
	dep.SetGroupVersionKind(GVKDeployment)
	require.NoError(t, cl.Get(context.Background(), depKey, dep))
}

func TestRunPlatform_DefaultTenantAppliesIPPResources(t *testing.T) {
	const (
		tenantName = "existing-team"
		appNs      = "ai-tenant-existing-team"
		gwNS       = "openshift-ingress"
		gwName     = "existing-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	// Simulates AITenantReconciler seed: every new MaasTenantConfig is seeded
	// with cleanup-complete at creation time.
	tenant.Annotations[AnnotationPayloadProcessingStatus] = PayloadProcessingStatusCleanupComplete
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		Source:     "aitenant",
	}

	var applied []appliedResource
	cl := runPlatformTestClient(t, scheme, []client.Object{
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName),
	}, &applied)

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.DeploymentPending, result.Detail)

	// Claiming for legacy deletes the status back to absent (legacy steady).
	var gotTenant maasv1alpha1.MaasTenantConfig
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: appNs, Name: maasv1alpha1.MaasTenantConfigInstanceName}, &gotTenant))
	assert.Equal(t, "", payloadProcessingStatus(&gotTenant))

	hasIPPDeployment := false
	hasIPPEnvoyFilter := false
	for _, res := range applied {
		if res.gvk == GVKDeployment && res.name == PayloadProcessingDeploymentName(tenantName) {
			hasIPPDeployment = true
		}
		if res.gvk == GVKEnvoyFilter && res.name == PayloadProcessingEnvoyFilterName(tenantName) {
			hasIPPEnvoyFilter = true
		}
	}
	assert.True(t, hasIPPDeployment, "expected payload-processing Deployment to be applied for the existing IPP path")
	assert.True(t, hasIPPEnvoyFilter, "expected payload-processing EnvoyFilter to be applied for the existing IPP path")
}

func TestRunPlatform_DefaultTenantReadyWithIPPEnvoyFilter(t *testing.T) {
	const (
		tenantName = "existing-team"
		appNs      = "ai-tenant-existing-team"
		gwNS       = "openshift-ingress"
		gwName     = "existing-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	tenant.Annotations[AnnotationPayloadProcessingStatus] = PayloadProcessingStatusCleanupComplete
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	priority := PayloadProcessingEnvoyFilterPriority
	ef := payloadProcessingEnvoyFilter(gwNS, PayloadProcessingEnvoyFilterName(tenantName), gwName, &priority)
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		Source:     "aitenant",
	}

	cl := runPlatformTestClient(t, scheme, []client.Object{
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), ef,
	}, nil)

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.DeploymentPending, result.Detail)
}

// TestRunPlatform_LegacyBlockedDuringInFlightSwap covers the race this whole
// TestRunPlatform_LegacyBlockedDuringInFlightSwap covers the race the swap
// handshake closes: a peer still owns the dataplane (opaque non-absent status)
// and has not finished switch-off cleanup. Legacy must wait — not apply IPP.
func TestRunPlatform_LegacyBlockedDuringInFlightSwap(t *testing.T) {
	const (
		tenantName = "existing-team"
		appNs      = "ai-tenant-existing-team"
		gwNS       = "openshift-ingress"
		gwName     = "existing-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	tenant.Annotations[AnnotationPayloadProcessingStatus] = "steady" // opaque peer claim
	mcfg := &maasv1alpha1.Config{
		ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")},
	}
	gateway := &gwapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS},
	}
	platformContext := PlatformContext{
		GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName},
		Source:     "aitenant",
	}

	var applied []appliedResource
	cl := runPlatformTestClient(t, scheme, []client.Object{
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName),
	}, &applied)

	result, err := RunPlatform(
		context.Background(),
		logr.Discard(),
		cl,
		scheme,
		tenant,
		platformContext,
		platformOverlayManifestPath(t),
		appNs,
		"controller-ns",
		"https://kubernetes.default.svc",
		"opendatahub",
		mcfg,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.DeploymentPending, "must wait for peer switch-off to write cleanup-complete")

	for _, res := range applied {
		if isIPPResource(res.gvk, res.name) {
			t.Fatalf("unexpected IPP resource applied while a peer still owns: %s %s/%s", res.gvk.String(), res.namespace, res.name)
		}
	}
}

func unstructuredIPPObject(gvk schema.GroupVersionKind, namespace, name string, annotations map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(namespace)
	obj.SetUID(types.UID("uid-" + name))
	obj.SetResourceVersion("1")
	if annotations != nil {
		obj.SetAnnotations(annotations)
	}
	return obj
}

func praxisOwnedDeployment(namespace, name string) *appsv1.Deployment {
	fieldV1, err := json.Marshal(map[string]any{
		"f:metadata": map[string]any{
			"f:name": map[string]any{},
		},
	})
	if err != nil {
		panic(err)
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			ManagedFields: []metav1.ManagedFieldsEntry{{
				Manager:    aiGatewayControllerFieldOwner,
				Operation:  metav1.ManagedFieldsOperationApply,
				APIVersion: "apps/v1",
				FieldsType: "FieldsV1",
				FieldsV1:   &metav1.FieldsV1{Raw: fieldV1},
			}},
		},
	}
}

func TestIPPResourcesForTenant_DefaultTenantUsesExistingNames(t *testing.T) {
	params := PlatformParams{
		GatewayNamespace: "openshift-ingress",
		TenantIdentifier: "",
	}
	refs := ippResourcesForTenant(params)
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.name)
	}
	assert.Contains(t, names, PayloadProcessingName)
	assert.NotContains(t, names, PayloadProcessingDeploymentName("redteam"))
}

func TestIPPResourcesForTenant_PerTenantSuffix(t *testing.T) {
	const tenantID = "praxis-team"
	params := PlatformParams{
		GatewayNamespace: "openshift-ingress",
		TenantIdentifier: tenantID,
	}
	refs := ippResourcesForTenant(params)
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.name)
		if ref.gvk != GVKClusterRoleBinding {
			assert.Equal(t, "openshift-ingress", ref.namespace, "namespaced IPP ref %s", ref.name)
		}
	}
	assert.Contains(t, names, PayloadProcessingDeploymentName(tenantID))
	assert.Contains(t, names, PayloadProcessingEnvoyFilterName(tenantID))
	assert.Contains(t, names, PayloadProcessingReaderClusterRoleBindingNameForTenant(tenantID))
}

func TestCleanupIPPResources_DeletesManagedResources(t *testing.T) {
	const (
		tenantID = "praxis-team"
		gwNS     = "openshift-ingress"
	)
	params := PlatformParams{
		GatewayNamespace: gwNS,
		ModelNamespace:   "tenant-ns",
		TenantIdentifier: tenantID,
	}
	scheme := praxisTestScheme(t)

	seed := []client.Object{
		unstructuredIPPObject(GVKDeployment, gwNS, PayloadProcessingDeploymentName(tenantID), nil),
		unstructuredIPPObject(GVKEnvoyFilter, gwNS, PayloadProcessingEnvoyFilterName(tenantID), nil),
		unstructuredIPPObject(GVKService, gwNS, PayloadProcessingServiceName(tenantID), nil),
	}
	for i := range seed {
		if obj, ok := seed[i].(*unstructured.Unstructured); ok {
			setConfigControllerOwnerRef(obj, types.UID("cfg-uid"))
		}
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed...).Build()

	err := cleanupIPPResources(context.Background(), cl, params, types.UID("cfg-uid"), logr.Discard())
	require.NoError(t, err)

	for _, ref := range []ippResourceRef{
		{gvk: GVKDeployment, namespace: gwNS, name: PayloadProcessingDeploymentName(tenantID)},
		{gvk: GVKEnvoyFilter, namespace: gwNS, name: PayloadProcessingEnvoyFilterName(tenantID)},
		{gvk: GVKService, namespace: gwNS, name: PayloadProcessingServiceName(tenantID)},
	} {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(ref.gvk)
		obj.SetName(ref.name)
		obj.SetNamespace(ref.namespace)
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
		assert.True(t, apierrors.IsNotFound(err), "expected %s/%s to be deleted", ref.namespace, ref.name)
	}
}

func TestCleanupIPPResources_DeletesUnmanagedResources(t *testing.T) {
	const (
		tenantID = "praxis-team"
		gwNS     = "openshift-ingress"
	)
	params := PlatformParams{
		GatewayNamespace: gwNS,
		ModelNamespace:   "tenant-ns",
		TenantIdentifier: tenantID,
	}
	scheme := praxisTestScheme(t)

	deployment := unstructuredIPPObject(
		GVKDeployment,
		gwNS,
		PayloadProcessingDeploymentName(tenantID),
		map[string]string{AnnotationManaged: "false"},
	)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()

	err := cleanupIPPResources(context.Background(), cl, params, types.UID("cfg-uid"), logr.Discard())
	require.NoError(t, err)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVKDeployment)
	key := types.NamespacedName{Namespace: gwNS, Name: PayloadProcessingDeploymentName(tenantID)}
	err = cl.Get(context.Background(), key, got)
	assert.True(t, apierrors.IsNotFound(err), "unmanaged IPP resources must be deleted on switch-off")
}

func TestCleanupIPPResources_SkipsPraxisOwnedResources(t *testing.T) {
	const (
		tenantID = "praxis-team"
		gwNS     = "openshift-ingress"
	)
	params := PlatformParams{
		GatewayNamespace: gwNS,
		ModelNamespace:   "tenant-ns",
		TenantIdentifier: tenantID,
	}
	scheme := praxisTestScheme(t)

	deployment := praxisOwnedDeployment(gwNS, PayloadProcessingDeploymentName(tenantID))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).Build()

	err := cleanupIPPResources(context.Background(), cl, params, types.UID("cfg-uid"), logr.Discard())
	require.NoError(t, err)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVKDeployment)
	key := types.NamespacedName{Namespace: gwNS, Name: PayloadProcessingDeploymentName(tenantID)}
	require.NoError(t, cl.Get(context.Background(), key, got))
}

func TestCleanupIPPResources_DeletesOnlyOwnedInferenceRouteAfterWritersStop(t *testing.T) {
	const ns = "llm"
	params := PlatformParams{AppNamespace: "maas-system", ModelNamespace: ns, GatewayNamespace: "openshift-ingress", TenantIdentifier: "team-a"}
	scheme := praxisTestScheme(t)
	uid := types.UID("inference-uid")
	model := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "inference.opendatahub.io/v1alpha1", "kind": "ExternalModel",
		"metadata": map[string]any{"name": "demo", "namespace": ns, "uid": string(uid)},
	}}
	model.SetGroupVersionKind(inferenceExternalModelRouteOwnerGVK)
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": map[string]any{
			"name": "demo", "namespace": ns,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": ippExternalModelManagedBy,
				ippExternalModelLabel:          "demo",
			},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "inference.opendatahub.io/v1alpha1", "kind": "ExternalModel", "name": "demo", "uid": string(uid), "controller": true,
			}},
		},
	}}
	route.SetGroupVersionKind(GVKHTTPRoute)
	route.SetUID(types.UID("route-uid"))
	route.SetResourceVersion("1")
	userRoute := route.DeepCopy()
	userRoute.SetName("user-route")
	userRoute.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "user"})
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, route, userRoute).Build()
	require.NoError(t, cleanupIPPResources(context.Background(), cl, params, types.UID("cfg"), logr.Discard()))
	deleted := &unstructured.Unstructured{}
	deleted.SetGroupVersionKind(GVKHTTPRoute)
	assert.True(t, apierrors.IsNotFound(cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "demo"}, deleted)))
	assert.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "user-route"}, deleted))
}

func TestCleanupIPPResources_DefersRouteWhileIPPWriterExists(t *testing.T) {
	const (
		ns        = "llm"
		gatewayNS = "openshift-ingress"
		tenantID  = "team-a"
	)
	params := PlatformParams{AppNamespace: "maas-system", ModelNamespace: ns, GatewayNamespace: gatewayNS, TenantIdentifier: tenantID}
	scheme := praxisTestScheme(t)
	model := ippExternalModel(ns, "demo", types.UID("model-uid"))
	route := ippOwnedExternalModelRoute(ns, "demo", types.UID("model-uid"))
	writer := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "writer-pod",
		Namespace: gatewayNS,
		Labels: map[string]string{
			LabelTenantInstance: PayloadProcessingDeploymentName(tenantID),
			"app":               PayloadProcessingName,
		},
	}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, route, writer).Build()
	err := cleanupIPPResources(context.Background(), cl, params, types.UID("cfg"), logr.Discard())
	assert.ErrorContains(t, err, "writer pod")
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVKHTTPRoute)
	assert.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "demo"}, got))
}

func ippOwnedExternalModelRoute(namespace, modelName string, uid types.UID) *unstructured.Unstructured {
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute",
		"metadata": map[string]any{
			"name": modelName, "namespace": namespace,
			"labels": map[string]any{
				"app.kubernetes.io/managed-by": ippExternalModelManagedBy,
				ippExternalModelLabel:          modelName,
			},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "inference.opendatahub.io/v1alpha1", "kind": "ExternalModel", "name": modelName, "uid": string(uid), "controller": true,
			}},
		},
	}}
	route.SetGroupVersionKind(GVKHTTPRoute)
	route.SetUID(types.UID("route-uid-" + modelName))
	route.SetResourceVersion("1")
	return route
}

func ippExternalModel(namespace, name string, uid types.UID) *unstructured.Unstructured {
	model := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "inference.opendatahub.io/v1alpha1", "kind": "ExternalModel",
		"metadata": map[string]any{"name": name, "namespace": namespace, "uid": string(uid)},
	}}
	model.SetGroupVersionKind(inferenceExternalModelRouteOwnerGVK)
	return model
}

func TestCleanupIPPExternalModelRoutes_PreservesUnsafeOwnership(t *testing.T) {
	const ns = "llm"
	params := PlatformParams{AppNamespace: "maas-system", ModelNamespace: ns}
	cases := map[string]struct {
		model  bool
		mutate func(*unstructured.Unstructured)
	}{
		"recreated model UID": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			refs[0].UID = types.UID("new-uid")
			route.SetOwnerReferences(refs)
		}},
		"missing model": {model: false},
		"missing controller owner": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			falseValue := false
			refs[0].Controller = &falseValue
			route.SetOwnerReferences(refs)
		}},
		"empty owner UID": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			refs[0].UID = ""
			route.SetOwnerReferences(refs)
		}},
		"wrong owner API version": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			refs[0].APIVersion = "inference.opendatahub.io/v1beta1"
			route.SetOwnerReferences(refs)
		}},
		"wrong owner kind": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			refs[0].Kind = "ExternalProvider"
			route.SetOwnerReferences(refs)
		}},
		"wrong owner name": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			refs[0].Name = "other"
			route.SetOwnerReferences(refs)
		}},
		"duplicate matching owners": {model: true, mutate: func(route *unstructured.Unstructured) {
			refs := route.GetOwnerReferences()
			route.SetOwnerReferences(append(refs, refs[0]))
		}},
		"SSA managed": {model: true, mutate: func(route *unstructured.Unstructured) {
			route.SetManagedFields([]metav1.ManagedFieldsEntry{{
				Manager:    aiGatewayControllerFieldOwner,
				Operation:  metav1.ManagedFieldsOperationApply,
				APIVersion: "gateway.networking.k8s.io/v1",
				FieldsType: "FieldsV1",
				FieldsV1:   &metav1.FieldsV1{Raw: []byte("{}")},
			}})
		}},
		"foreign route": {model: true, mutate: func(route *unstructured.Unstructured) {
			route.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "user"})
		}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			route := ippOwnedExternalModelRoute(ns, "demo", types.UID("old-uid"))
			if tc.mutate != nil {
				tc.mutate(route)
			}
			if name == "SSA managed" {
				require.True(t, hasSSAFieldManager(route, aiGatewayControllerFieldOwner))
			}
			objects := []client.Object{route}
			if tc.model {
				objects = append(objects, ippExternalModel(ns, "demo", types.UID("old-uid")))
			}
			builder := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(objects...)
			if name == "SSA managed" {
				builder = builder.WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if err := c.List(ctx, list, opts...); err != nil {
							return err
						}
						items, ok := list.(*unstructured.UnstructuredList)
						if ok && len(items.Items) == 1 {
							items.Items[0].SetManagedFields(route.GetManagedFields())
						}
						return nil
					},
				})
			}
			cl := builder.Build()
			require.NoError(t, cleanupIPPExternalModelRoutes(context.Background(), cl, params, logr.Discard()))
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(GVKHTTPRoute)
			assert.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "demo"}, got))
		})
	}
}

func TestEnsureIPPWritersStopped_DefersForTerminatingDeploymentAndRemainingPod(t *testing.T) {
	const (
		ns       = "openshift-ingress"
		tenantID = "team-a"
	)
	params := PlatformParams{GatewayNamespace: ns, TenantIdentifier: tenantID}
	deletion := metav1.Now()
	deployment := unstructuredIPPObject(GVKDeployment, ns, PayloadProcessingDeploymentName(tenantID), nil)
	setConfigControllerOwnerRef(deployment, types.UID("cfg"))
	deployment.SetFinalizers([]string{"test/finalizer"})
	deployment.SetDeletionTimestamp(&deletion)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "writer-pod", Namespace: ns,
		Labels: map[string]string{LabelTenantInstance: PayloadProcessingDeploymentName(tenantID)},
	}}
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(deployment, pod).Build()
	err := ensureIPPWritersStopped(context.Background(), cl, params, types.UID("cfg"))
	assert.ErrorContains(t, err, "terminating")

	cl = fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(pod).Build()
	err = ensureIPPWritersStopped(context.Background(), cl, params, types.UID("cfg"))
	assert.ErrorContains(t, err, "writer pod")
}

func TestEnsureIPPWritersStoppedChecksPodsIndependentOfDeploymentOwnership(t *testing.T) {
	const (
		ns       = "openshift-ingress"
		tenantID = "team-a"
	)
	params := PlatformParams{GatewayNamespace: ns, TenantIdentifier: tenantID}
	pod := func(name, app string, extra map[string]string) *corev1.Pod {
		labels := map[string]string{
			LabelTenantInstance: PayloadProcessingDeploymentName(tenantID),
			"app":               app,
		}
		for key, value := range extra {
			labels[key] = value
		}
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels}}
	}

	tests := map[string]struct {
		deployment client.Object
		pod        *corev1.Pod
		wantError  string
	}{
		"absent deployment with writer pod": {
			pod:       pod("writer", PayloadProcessingName, nil),
			wantError: "writer pod",
		},
		"foreign deployment with writer pod": {
			deployment: unstructuredIPPObject(GVKDeployment, ns, PayloadProcessingDeploymentName(tenantID), map[string]string{AnnotationManaged: "false"}),
			pod:        pod("writer", PayloadProcessingName, nil),
			wantError:  "still present",
		},
		"Praxis deployment with writer pod": {
			deployment: func() client.Object {
				return praxisOwnedDeployment(ns, PayloadProcessingDeploymentName(tenantID))
			}(),
			pod:       pod("writer", PayloadProcessingName, nil),
			wantError: "writer pod",
		},
		"unmanaged deployment without writer pod still blocks": {
			deployment: unstructuredIPPObject(GVKDeployment, ns, PayloadProcessingDeploymentName(tenantID), map[string]string{AnnotationManaged: "false"}),
			wantError:  "still present",
		},
		"Praxis pod is excluded": {
			pod: pod("praxis", PayloadProcessingName, map[string]string{"app.kubernetes.io/managed-by": aiGatewayControllerFieldOwner}),
		},
		"unrelated tenant pod is excluded": {
			pod: pod("unrelated", "other-workload", nil),
		},
		"ambiguous tenant pod fails closed": {
			pod: func() *corev1.Pod {
				result := pod("ambiguous", "", nil)
				delete(result.Labels, "app")
				return result
			}(),
			wantError: "ambiguous identity",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			objects := []client.Object{}
			if tt.deployment != nil {
				objects = append(objects, tt.deployment)
			}
			if tt.pod != nil {
				objects = append(objects, tt.pod)
			}
			cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(objects...).Build()
			err := ensureIPPWritersStopped(context.Background(), cl, params, types.UID("cfg"))
			if tt.wantError == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}

func TestRunPlatformDoesNotMarkCleanupCompleteWhileWriterPodRemains(t *testing.T) {
	const (
		tenantName = "praxis-team"
		appNs      = "ai-tenant-praxis-team"
		gwNS       = "openshift-ingress"
		gwName     = "praxis-gateway"
	)
	scheme := praxisTestScheme(t)
	tenant := praxisTenantConfig(appNs, tenantName)
	mcfg := &maasv1alpha1.Config{ObjectMeta: metav1.ObjectMeta{Name: maasv1alpha1.ConfigInstanceName, UID: types.UID("cfg-uid")}}
	gateway := &gwapiv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: gwNS}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "writer-pod",
		Namespace: gwNS,
		Labels: map[string]string{
			LabelTenantInstance: PayloadProcessingDeploymentName(tenantName),
			"app":               PayloadProcessingName,
		},
	}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		mcfg, gateway, tenant, readyMaaSAPIDeployment(appNs, tenantName), pod,
	).Build()

	_, err := RunPlatform(
		context.Background(), logr.Discard(), cl, scheme, tenant,
		PlatformContext{GatewayRef: maasv1alpha1.TenantGatewayRef{Namespace: gwNS, Name: gwName}, SkipIPP: true, Source: "aitenant"},
		platformOverlayManifestPath(t), appNs, "controller-ns", "https://kubernetes.default.svc", "opendatahub", mcfg,
	)
	require.ErrorContains(t, err, "writer pod")
	persistedTenant := &maasv1alpha1.MaasTenantConfig{}
	require.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(tenant), persistedTenant))
	assert.False(t, isPayloadProcessingCleanupComplete(persistedTenant), "cleanup must not be marked complete while a writer remains")
}

func TestCleanupIPPExternalModelRoutes_IsNamespaceScoped(t *testing.T) {
	const (
		ns      = "llm"
		infraNS = "maas-system"
	)
	route := ippOwnedExternalModelRoute(ns, "demo", types.UID("uid"))
	sharedRoute := ippOwnedExternalModelRoute(infraNS, "shared", types.UID("shared-uid"))
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(
		route,
		sharedRoute,
		ippExternalModel(ns, "demo", types.UID("uid")),
		ippExternalModel(infraNS, "shared", types.UID("shared-uid")),
	).Build()
	require.NoError(t, cleanupIPPExternalModelRoutes(context.Background(), cl, PlatformParams{AppNamespace: infraNS, ModelNamespace: ns}, logr.Discard()))

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVKHTTPRoute)
	assert.True(t, apierrors.IsNotFound(cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "demo"}, got)))
	assert.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: infraNS, Name: "shared"}, got), "a model in shared infrastructure is not proven to belong exclusively to this tenant")
}

func TestCleanupIPPExternalModelRoutesRequiresModelNamespace(t *testing.T) {
	err := cleanupIPPExternalModelRoutes(context.Background(), fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).Build(), PlatformParams{AppNamespace: "maas-system"}, logr.Discard())
	assert.ErrorContains(t, err, "model namespace is required")
}

func TestDeleteIPPResourceIfManaged_UsesForegroundPropagationForWriters(t *testing.T) {
	const ns = "openshift-ingress"
	deployment := unstructuredIPPObject(GVKDeployment, ns, PayloadProcessingDeploymentName("team-a"), nil)
	setConfigControllerOwnerRef(deployment, types.UID("cfg"))
	var propagation *metav1.DeletionPropagation
	var preconditions *metav1.Preconditions
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(deployment).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
			propagation = deleteOptions.PropagationPolicy
			preconditions = deleteOptions.Preconditions
			return c.Delete(ctx, obj, opts...)
		},
	}).Build()
	require.NoError(t, deleteIPPResourceIfManaged(context.Background(), cl, ippResourceRef{
		gvk: GVKDeployment, namespace: ns, name: deployment.GetName(),
	}, types.UID("cfg"), logr.Discard()))
	require.NotNil(t, propagation)
	assert.Equal(t, metav1.DeletePropagationForeground, *propagation)
	require.NotNil(t, preconditions)
	assert.Equal(t, deployment.GetUID(), *preconditions.UID)
	assert.Equal(t, deployment.GetResourceVersion(), *preconditions.ResourceVersion)
}

func TestCleanupIPPExternalModelRoutesPropagatesDeletePreconditionFailure(t *testing.T) {
	const ns = "llm"
	route := ippOwnedExternalModelRoute(ns, "demo", types.UID("uid"))
	model := ippExternalModel(ns, "demo", types.UID("uid"))
	conflict := apierrors.NewConflict(schema.GroupResource{Group: GVKHTTPRoute.Group, Resource: "httproutes"}, route.GetName(), errors.New("resource changed"))
	cl := fake.NewClientBuilder().WithScheme(praxisTestScheme(t)).WithObjects(route, model).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
			require.NotNil(t, deleteOptions.Preconditions)
			require.Equal(t, route.GetUID(), *deleteOptions.Preconditions.UID)
			require.Equal(t, route.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)
			return conflict
		},
	}).Build()

	err := cleanupIPPExternalModelRoutes(context.Background(), cl, PlatformParams{AppNamespace: "maas-system", ModelNamespace: ns}, logr.Discard())
	assert.ErrorIs(t, err, conflict)
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GVKHTTPRoute)
	assert.NoError(t, cl.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: route.GetName()}, got))
}
