package filter

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRequiredNodeAffinityMatchesTerms(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "accelerator",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"npu"},
									},
									{
										Key:      "generation",
										Operator: corev1.NodeSelectorOpGt,
										Values:   []string{"2"},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"accelerator": "npu",
				"generation":  "3",
			},
		},
	}

	if !requiredNodeAffinityMatches(pod, node) {
		t.Fatal("matching node affinity was rejected")
	}

	node.Labels["generation"] = "1"

	if requiredNodeAffinityMatches(pod, node) {
		t.Fatal("non-matching numeric node affinity passed")
	}
}

func TestRequiredNodeAffinityMatchesMetadataName(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchFields: []corev1.NodeSelectorRequirement{
									{
										Key:      "metadata.name",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"node-a"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if !requiredNodeAffinityMatches(pod, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}) {
		t.Fatal("metadata.name affinity did not match")
	}

	if requiredNodeAffinityMatches(pod, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}) {
		t.Fatal("metadata.name affinity matched wrong node")
	}
}
