package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
)

func main() {
	// Create a sample Audit Event
	event := auditv1.Event{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Event",
			APIVersion: "audit.k8s.io/v1",
		},
		Level:      auditv1.LevelRequestResponse,
		AuditID:    types.UID(uuid.New().String()),
		Stage:      auditv1.StageResponseComplete,
		RequestURI: "/api/v1/namespaces/default/configmaps",
		Verb:       "create",
		User: authnv1.UserInfo{
			Username: "kubernetes-admin",
			Groups:   []string{"system:masters", "system:authenticated"},
		},
		SourceIPs: []string{"127.0.0.1"},
		ObjectRef: &auditv1.ObjectReference{
			Resource:   "configmaps",
			Namespace:  "default",
			Name:       "test-configmap",
			APIVersion: "v1",
		},
		ResponseStatus: &metav1.Status{
			Code: 201,
		},
		RequestReceivedTimestamp: metav1.NewMicroTime(time.Now()),
		StageTimestamp:           metav1.NewMicroTime(time.Now()),
		Annotations:              map[string]string{"authorization.k8s.io/decision": "allow"},
	}

	// Wrap it in an EventList (this is what the webhook sends)
	eventList := auditv1.EventList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EventList",
			APIVersion: "audit.k8s.io/v1",
		},
		Items: []auditv1.Event{event},
	}

	jsonData, err := json.Marshal(eventList)
	if err != nil {
		log.Fatalf("Failed to marshal event list: %v", err)
	}

	// Send to Vector sidecar (assuming port-forward to 8080)
	url := "http://localhost:8080/events"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Vector returned non-OK status: %s", resp.Status)
	}

	fmt.Printf("Successfully sent audit event (ID: %s) to Vector at %s\n", event.AuditID, url)
}
