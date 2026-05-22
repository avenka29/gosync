package gosync

import (
	"errors"
	"testing"
)

func TestBroadcastEvent_Validation(t *testing.T) {
	server := NewServer()

	tests := []struct {
		name    string
		evtName string
		data    interface{}
		wantErr error
	}{
		{
			name:    "Empty Event Name",
			evtName: "",
			data:    "some data",
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Nil Data",
			evtName: "chat",
			data:    nil,
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Non-Serializable Data (Function)",
			evtName: "chat",
			data:    func() {}, // Functions cannot be marshaled to JSON
			wantErr: ErrServerMsgInvalid,
		},
		{
			name:    "Valid Data",
			evtName: "chat",
			data:    "hello",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.BroadcastEvent(tt.evtName, tt.data)
			
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("BroadcastEvent() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Errorf("BroadcastEvent() unexpected error = %v", err)
				}
			}
		})
	}
}
