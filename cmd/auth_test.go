package main

import "testing"

func TestSelectAuthMode(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		want    authMode
		wantErr bool
	}{
		{name: "key only", cfg: Config{StorageAccountKey: "k"}, want: authModeSharedKey},
		{name: "key wins over sub and rg", cfg: Config{StorageAccountKey: "k", SubscriptionID: "s", StorageResourceGroup: "rg"}, want: authModeSharedKey},
		{name: "key with only sub", cfg: Config{StorageAccountKey: "k", SubscriptionID: "s"}, want: authModeSharedKey},
		{name: "sub and rg use ARM lookup", cfg: Config{SubscriptionID: "s", StorageResourceGroup: "rg"}, want: authModeARMKey},
		{name: "nothing set uses Entra ID", cfg: Config{}, want: authModeEntraID},
		{name: "only sub is an error", cfg: Config{SubscriptionID: "s"}, wantErr: true},
		{name: "only rg is an error", cfg: Config{StorageResourceGroup: "rg"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectAuthMode(&tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("mode = %q, want %q", got, tc.want)
			}
		})
	}
}
