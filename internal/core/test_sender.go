package core

type TestSender struct {
	sender *SenderKafka
}

func NewTestSender(kafka *SenderKafka) *TestSender {
	return &TestSender{sender: kafka}
}

func (ts *TestSender) Send(m []byte, s *string) {
	ts.sender.Send(m, s)
}
func (ts *TestSender) Start(s string) {
	select {}
}
