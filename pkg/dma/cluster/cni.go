package cluster

import (
	"context"
	"fmt"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const CniSubjectKey = "cni_subject"

func SubjectString(subject rbacv1.Subject) string {
	if subject.Kind == rbacv1.ServiceAccountKind {
		return subject.Kind + "/" + subject.Namespace + "/" + subject.Name
	}
	return subject.Kind + "/" + subject.Name
}

func ParseSubject(value string) (rbacv1.Subject, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	switch {
	case len(parts) == 3 && parts[0] == rbacv1.ServiceAccountKind && parts[1] != "" && parts[2] != "":
		return rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: parts[1], Name: parts[2]}, nil
	case len(parts) == 2 && parts[0] == rbacv1.UserKind && parts[1] != "":
		return rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: parts[1]}, nil
	}
	return rbacv1.Subject{}, fmt.Errorf("subject %q is neither of the form ServiceAccount/<namespace>/<name> nor User/<name>", value)
}

func LoadCniSubject(ctx context.Context, client kubernetes.Interface, namespace, name string) (*rbacv1.Subject, error) {
	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read configuration ConfigMap %s/%s: %w", namespace, name, err)
	}
	value := strings.TrimSpace(configMap.Data[CniSubjectKey])
	if value == "" {
		return nil, nil
	}
	subject, err := ParseSubject(value)
	if err != nil {
		return nil, fmt.Errorf("configuration ConfigMap %s/%s is invalid: %w", namespace, name, err)
	}
	return &subject, nil
}
