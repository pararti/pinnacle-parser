package core

import (
	"context"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"github.com/pararti/pinnacle-parser/internal/abstruct"
	"github.com/pararti/pinnacle-parser/internal/models/parsed"
	"github.com/pararti/pinnacle-parser/internal/options"
	"github.com/pararti/pinnacle-parser/internal/storage"
	"github.com/pararti/pinnacle-parser/pkg/logger"
)

const matchSuffix = "related"
const straightSuffix = "straight"
const maxTime = 1<<63 - 1 //290 years
const betBatchSize = 8

type Engine struct {
	logger    *logger.Logger
	Sender    *abstruct.Sender
	Storage   *storage.MapStorage
	matchChan chan []byte
	betChan   chan []byte
}

func NewEngine(l *logger.Logger, s *storage.MapStorage) *Engine {
	return &Engine{
		logger:    l,
		Storage:   s,
		matchChan: make(chan []byte, 10),
		betChan:   make(chan []byte, 10),
	}
}

func (e *Engine) Start(appOpts *options.Options) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserDataDir(appOpts.CookieDir),
		chromedp.UserAgent(appOpts.UserAgent),
		chromedp.Flag("remote-allow-origins", "*"),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("use-automation-extension", false),
		chromedp.Flag("password-store", "basic"),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-component-extensions-with-background-pages", true),
		chromedp.Flag("ignore-certificate-errors", true),
		chromedp.Flag("disable-web-security", true),
		chromedp.Flag("allow-insecure-localhost", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.WindowSize(1920, 1200),

		//chromedp.Flag("headless", false),
	)

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	defer cancelAlloc()

	ctx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()

	// Enable network events
	if err := chromedp.Run(ctx, network.Enable()); err != nil {
		e.logger.Fatal("Failed to enable network events: %v", err)
	}

	go e.processMatches()
	go e.processBets()

	e.logger.Info("Запуск браузерного движка и прослушивания событий")

	chromedp.ListenTarget(ctx, func(ev interface{}) {
		if response, ok := ev.(*network.EventResponseReceived); ok {
			if response.Type == network.ResourceTypeFetch {
				if strings.HasSuffix(response.Response.URL, matchSuffix) {
					go func(requestID network.RequestID) {
						var body []byte
						err := chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
							var err error
							body, err = network.GetResponseBody(requestID).Do(ctx)

							return err
						}))
						if err != nil {
							e.logger.Warn("Failed to get body:", err)
							return
						}
						e.matchChan <- body
					}(response.RequestID)
				} else if strings.HasSuffix(response.Response.URL, straightSuffix) {
					go func(requestID network.RequestID) {
						var body []byte
						err := chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
							var err error
							body, err = network.GetResponseBody(requestID).Do(ctx)
							return err
						}))
						if err != nil {
							e.logger.Warn("Failed to get body:", err)
							return
						}
						e.betChan <- body
					}(response.RequestID)
				}

			}
		}
	})

	// Navigate to target website and wait for XHR requests
	if err := chromedp.Run(ctx,
		chromedp.Navigate(appOpts.Site),
		chromedp.Sleep(time.Duration(maxTime)),
	); err != nil {
		e.logger.Fatal("Navigation error: %v", err)
	}
}

func (e *Engine) processMatches() {
	for body := range e.matchChan {
		var matches []*parsed.Match
		if err := sonic.Unmarshal(body, &matches); err != nil {
			e.logger.Error("Failed to unmarshal match data:", err)
		} else {
			e.Storage.SetMatches(matches)
		}
	}
}

func (e *Engine) processBets() {
	batches := make(map[int][]*parsed.Straight)
	for body := range e.betChan {
		var bets []*parsed.Straight
		if err := sonic.Unmarshal(body, &bets); err != nil {
			e.logger.Error("Failed to unmarshal bet data:", err)
		} else {
			if len(bets) > 0 {
				batches[bets[0].MatchupID] = bets
				e.Storage.SetBets(batches)
			}
		}
	}
}
