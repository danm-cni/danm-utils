package v43to44

import (
	"embed"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

const (
	OldGroup = "danm.k8s.io"
	NewGroup = "danm.io"
	Version  = "v1"
)

//go:embed manifests/crds
var crdManifests embed.FS

type apiKind struct {
	Kind       string
	Plural     string
	Namespaced bool
}

var (
	kindDanmEp         = apiKind{Kind: "DanmEp", Plural: "danmeps", Namespaced: true}
	kindDanmNet        = apiKind{Kind: "DanmNet", Plural: "danmnets", Namespaced: true}
	kindTenantNetwork  = apiKind{Kind: "TenantNetwork", Plural: "tenantnetworks", Namespaced: true}
	kindClusterNetwork = apiKind{Kind: "ClusterNetwork", Plural: "clusternetworks"}
	kindTenantConfig   = apiKind{Kind: "TenantConfig", Plural: "tenantconfigs"}
	kindReservedIP     = apiKind{Kind: "ReservedIP", Plural: "reservedips", Namespaced: true}
)

var oldKinds = []apiKind{kindDanmEp, kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig}

var productionOnlyKinds = []apiKind{kindTenantNetwork, kindClusterNetwork, kindTenantConfig}

func (kind apiKind) OldCrdName() string {
	return kind.Plural + "." + OldGroup
}

func (kind apiKind) NewCrdName() string {
	return kind.Plural + "." + NewGroup
}

func (kind apiKind) OldResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: OldGroup, Version: Version, Resource: kind.Plural}
}

func (kind apiKind) NewResource() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: NewGroup, Version: Version, Resource: kind.Plural}
}

func (kind apiKind) Scope() apiextensionsv1.ResourceScope {
	if kind.Namespaced {
		return apiextensionsv1.NamespaceScoped
	}
	return apiextensionsv1.ClusterScoped
}

func sourceKinds(mode cluster.Mode) []apiKind {
	if mode == cluster.ModeProduction {
		return []apiKind{kindDanmEp, kindTenantNetwork, kindClusterNetwork, kindTenantConfig}
	}
	return []apiKind{kindDanmEp, kindDanmNet}
}

func targetKinds(mode cluster.Mode) []apiKind {
	if mode == cluster.ModeProduction {
		return append(sourceKinds(mode), kindReservedIP)
	}
	return sourceKinds(mode)
}

func targetCrd(mode cluster.Mode, kind apiKind) (*apiextensionsv1.CustomResourceDefinition, error) {
	path := fmt.Sprintf("manifests/crds/%s/%s.yaml", mode, kind.Kind)
	raw, err := crdManifests.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no %s CRD for %s mode: %w", kind.Kind, mode, err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.UnmarshalStrict(raw, crd); err != nil {
		return nil, fmt.Errorf("the embedded %s CRD for %s mode is malformed: %w", kind.Kind, mode, err)
	}
	return crd, nil
}

func targetSchema(mode cluster.Mode, kind apiKind) (*structuralschema.Structural, error) {
	crd, err := targetCrd(mode, kind)
	if err != nil {
		return nil, err
	}
	for _, version := range crd.Spec.Versions {
		if version.Name != Version || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		internal := &apiextensions.JSONSchemaProps{}
		if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(version.Schema.OpenAPIV3Schema, internal, nil); err != nil {
			return nil, fmt.Errorf("the schema of the embedded %s CRD cannot be converted: %w", kind.Kind, err)
		}
		structural, err := structuralschema.NewStructural(internal)
		if err != nil {
			return nil, fmt.Errorf("the schema of the embedded %s CRD is not structural: %w", kind.Kind, err)
		}
		return structural, nil
	}
	return nil, fmt.Errorf("the embedded %s CRD has no schema for version %s", kind.Kind, Version)
}
