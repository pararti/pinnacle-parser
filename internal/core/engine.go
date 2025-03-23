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

	var allocCtx context.Context
	var cancelAlloc context.CancelFunc

	if appOpts.RemoteChromeURL != "" {
		// Connect to remote Chrome instance
		e.logger.Info("Подключение к удаленному Chrome по адресу %s", appOpts.RemoteChromeURL)
		allocCtx, cancelAlloc = chromedp.NewRemoteAllocator(context.Background(), appOpts.RemoteChromeURL, chromedp.NoModifyURL)
	} else {
		// Use local Chrome instance
		allocCtx, cancelAlloc = chromedp.NewExecAllocator(context.Background(), opts...)
	}
	defer cancelAlloc()

	ctx, cancel := chromedp.NewContext(allocCtx)
	defer cancel()

	e.logger.Info("Проверка статуса авторизации...")

	var isAuthenticated bool
	err := chromedp.Run(ctx,
		chromedp.Navigate(appOpts.Site),
		chromedp.Sleep(5*time.Second),
		chromedp.Evaluate(`
		(function() {
			if (document.querySelector('input#password')) {
				return false;
			}
			if (document.querySelector('input#username')) {
				return false;
			}

			return true;
		})()
	`, &isAuthenticated))

	if err != nil {
		e.logger.Fatal("Ошибка проверки авторизации:", err)
	}

	if isAuthenticated {
		e.logger.Info("Пользователь уже авторизован. Продолжаем работу...")
	} else {
		e.logger.Warn("Требуется авторизация")
		e.logger.Info("Выполняем процесс авторизации...")
		e.performLogin(ctx, appOpts.Login, appOpts.Password, appOpts.Site)
	}

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
		chromedp.Reload(),
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

func (e *Engine) performLogin(ctx context.Context, username, password, link string) {
	err := chromedp.Run(ctx,
		chromedp.Navigate(link),
		chromedp.WaitVisible("input#username", chromedp.ByQuery),
		chromedp.SendKeys("input#username", username, chromedp.ByQuery),
		chromedp.SendKeys("input#password", password, chromedp.ByQuery),
		chromedp.Click(`div[data-test-id="header-login-loginButton"] button`, chromedp.ByQuery),
		chromedp.WaitNotPresent("input#password", chromedp.ByQuery),
	)

	if err != nil {
		e.logger.Fatal("Ошибка авторизации: ", err)
		return
	}

	e.logger.Info("Успешная авторизация! Ожидаем появления модального окна...")

	modalSelector := `button[data-test-id="Button"][type="button"][class*="button-l9TRHt6rdY fullWidth-RjvaOdiHkK ellipsis medium-sdlPvkH2AX dead-center ghostOnLight-DuD1oNNBJh"]`

	err = chromedp.Run(ctx, chromedp.Sleep(3*time.Second))
	if err != nil {
		e.logger.Warn("Ошибка при ожидании: %v", err)
	}

	err = chromedp.Run(ctx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			e.logger.Error("Пытаемся найти кнопку 'Do it later' с точным селектором")
			return nil
		}),
		chromedp.Click(modalSelector, chromedp.ByQuery, chromedp.NodeVisible),
	)

	if err != nil {
		e.logger.Error("Не удалось найти кнопку с точным селектором: %v", err)
		e.logger.Error("Пробуем альтернативный селектор")

		err = chromedp.Run(ctx,
			chromedp.Click(`button[data-test-id="Button"]:has(span:contains("Do it later"))`, chromedp.ByQuery, chromedp.NodeVisible),
		)

		if err != nil {
			e.logger.Error("Альтернативный селектор не сработал: %v", err)
			e.logger.Error("Пробуем еще один вариант")

			err = chromedp.Run(ctx,
				chromedp.Click(`button:has(span:contains("Do it later"))`, chromedp.ByQuery, chromedp.NodeVisible),
			)
		}
	}

	if err != nil {
		e.logger.Warn("Не удалось закрыть модальное окно: %v. Продолжаем работу...", err)
	} else {
		e.logger.Info("Успешно закрыли модальное окно!")
	}
}
