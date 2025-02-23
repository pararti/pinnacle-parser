package abstruct

type Sender interface {
	Send([]byte, *string)
	Start(string)
}
