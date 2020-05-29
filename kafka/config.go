package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/tools/tls"
)

type configBuilder struct {
	config *sarama.Config
	errors []error
}

func NewConfigBuilder() *configBuilder {
	return &configBuilder{
		config: sarama.NewConfig(),
	}
}
func (b *configBuilder) Build() (*sarama.Config, []error) {
	return b.config, b.errors
}
func (b *configBuilder) DefaultProducer() *configBuilder {
	b.config.Producer.Retry.Max = 5
	b.config.Producer.RequiredAcks = sarama.WaitForAll
	b.config.Producer.Return.Successes = true
	return b
}
func (b *configBuilder) AddTls(tlsClientCert string, tlsClientKey string, tlsSkipVerify bool) error {
	tlsConfig, err := tls.NewConfig(tlsClientCert, tlsClientKey)
	if err != nil {
		b.errors = append(b.errors, err)
	}

	b.config.Net.TLS.Enable = true
	b.config.Net.TLS.Config = tlsConfig
	b.config.Net.TLS.Config.InsecureSkipVerify = tlsSkipVerify

	return nil
}
