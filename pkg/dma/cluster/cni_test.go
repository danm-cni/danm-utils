package cluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestParseSubject(t *testing.T) {
	for value, expected := range map[string]string{
		"ServiceAccount/kube-system/danm": "ServiceAccount/kube-system/danm",
		" User/danm-cni ":                 "User/danm-cni",
	} {
		subject, err := ParseSubject(value)
		if err != nil || SubjectString(subject) != expected {
			t.Errorf("parsing %q returned %+v, %v", value, subject, err)
		}
	}
	for _, value := range []string{"ServiceAccount/danm", "User/", "Group/danm", "danm"} {
		if _, err := ParseSubject(value); err == nil {
			t.Errorf("parsing %q succeeded", value)
		}
	}
}

func TestLoadCniSubject(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data:       map[string]string{CniSubjectKey: "User/danm-cni"},
	})
	subject, err := LoadCniSubject(context.Background(), client, "kube-system", "dma-config")
	if err != nil || subject == nil || SubjectString(*subject) != "User/danm-cni" {
		t.Fatalf("loaded %v, %v", subject, err)
	}
	subject, err = LoadCniSubject(context.Background(), kubefake.NewSimpleClientset(), "kube-system", "dma-config")
	if err != nil || subject != nil {
		t.Fatalf("a missing ConfigMap returned %v, %v", subject, err)
	}
}

func TestLoadCniSubjectRejectsABadSubject(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data:       map[string]string{CniSubjectKey: "Group/danm"},
	})
	if _, err := LoadCniSubject(context.Background(), client, "kube-system", "dma-config"); err == nil {
		t.Fatal("a Group subject was accepted")
	}
}
