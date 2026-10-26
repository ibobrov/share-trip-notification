package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	ConsumeTotal    *prometheus.CounterVec
	ConsumeDuration *prometheus.HistogramVec
	InboxDuplicate  prometheus.Counter
	PersistTotal    *prometheus.CounterVec
	PersistDuration *prometheus.HistogramVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ConsumeTotal:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_consume_total", Help: "Kafka handling attempts; success and duplicate include successful offset commit."}, []string{"result"}),
		ConsumeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_consume_duration_seconds", Help: "Kafka handling duration including offset commit."}, []string{"result"}),
		InboxDuplicate:  prometheus.NewCounter(prometheus.CounterOpts{Name: "notification_inbox_duplicate_total", Help: "Duplicate events rejected by the committed inbox transaction."}),
		PersistTotal:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_persist_total", Help: "Notification persistence outcomes, not external SMS/email delivery. Created means committed in PostgreSQL."}, []string{"result"}),
		PersistDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_persist_duration_seconds", Help: "Notification persistence transaction duration; no external provider is configured."}, []string{"result"}),
	}
	reg.MustRegister(m.ConsumeTotal, m.ConsumeDuration, m.InboxDuplicate, m.PersistTotal, m.PersistDuration)
	for _, result := range []string{"success", "duplicate", "ignored", "error"} {
		m.ConsumeTotal.WithLabelValues(result)
		m.ConsumeDuration.WithLabelValues(result)
	}
	for _, result := range []string{"created", "error"} {
		m.PersistTotal.WithLabelValues(result)
		m.PersistDuration.WithLabelValues(result)
	}
	return m
}
