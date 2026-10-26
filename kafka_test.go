package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
	"github.com/Averlex/kafkakit/mocks"
)

// region Suites

func TestNewBrokerSuite(t *testing.T) {
	suite.Run(t, new(NewBrokerSuite))
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

func TestConnectSuite(t *testing.T) {
	suite.Run(t, new(ConnectSuite))
}

func TestCloseSuite(t *testing.T) {
	suite.Run(t, new(CloseSuite))
}

type NewBrokerSuite struct {
	suite.Suite
}

type ConfigSuite struct {
	suite.Suite
}

type ConnectSuite struct {
	suite.Suite
	mockCtrl    *mocks.MockController
	mockFactory *mocks.MockControllerFactory
}

type CloseSuite struct {
	suite.Suite
	mockCtrl    *mocks.MockController
	mockFactory *mocks.MockControllerFactory
	mockSub     *mocks.MockSubscription
}

func (s *ConnectSuite) SetupTest() {
	s.mockCtrl = mocks.NewMockController(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil controller but got nil")
	s.mockFactory = mocks.NewMockControllerFactory(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil factory but got nil")
}

func (s *ConnectSuite) TearDownTest() {
	s.Require().True(s.mockCtrl.AssertExpectations(s.T()), "controller expectations failed")
	s.Require().True(s.mockFactory.AssertExpectations(s.T()), "factory expectations failed")
	s.mockCtrl = nil
	s.mockFactory = nil
}

func (s *CloseSuite) SetupTest() {
	s.mockCtrl = mocks.NewMockController(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil controller but got nil")
	s.mockFactory = mocks.NewMockControllerFactory(s.T())
	s.Require().NotNil(s.mockFactory, "expected not nil factory but got nil")
	s.mockSub = mocks.NewMockSubscription(s.T())
	s.Require().NotNil(s.mockSub, "expected not nil subscription but got nil")
}

func (s *CloseSuite) TearDownTest() {
	s.Require().True(s.mockCtrl.AssertExpectations(s.T()), "controller expectations failed")
	s.Require().True(s.mockFactory.AssertExpectations(s.T()), "factory expectations failed")
	s.Require().True(s.mockSub.AssertExpectations(s.T()), "subscription expectations failed")
	s.mockCtrl = nil
	s.mockFactory = nil
	s.mockSub = nil
}

// endregion

// region Constants

const (
	testDSN      = "localhost:9092"
	testGroupID  = "test-group"
	testMaxBytes = 1024
)

// endregion

// region Tests

func (s *ConfigSuite) TestDefaultConfig() {
	cfg := kafka.DefaultConfig()

	s.Require().Equal(kafka.DefaultMode, cfg.Mode, "default mode mismatch")
	s.Require().Equal(kafka.DefaultMaxBytes, cfg.MaxBytes, "default maximum message size mismatch")
	s.Require().Equal(kafka.DefaultBatchSize, cfg.BatchSize, "default batch size mismatch")
	s.Require().Equal(kafka.DefaultBatchBytes, cfg.BatchBytes, "default batch byte limit mismatch")
	s.Require().Equal(kafka.DefaultBatchTimeout, cfg.BatchTimeout, "default batch timeout mismatch")
	s.Require().Equal(kafka.DefaultTopicCreateAttempts, cfg.TopicCreateAttempts, "default topic creation attempts mismatch")
	s.Require().Equal(kafka.DefaultTopicCreateBackoff, cfg.TopicCreateBackoff, "default topic creation backoff mismatch")
	s.Require().Equal(kafka.DefaultNackBackoff, cfg.NackBackoff, "default nack backoff mismatch")
	s.Require().Equal(kafka.DefaultAutoCommit, cfg.AutoCommit, "default offset auto commit mismatch")
	s.Require().Equal(kafka.DefaultConnectionTest, cfg.ConnectionTest, "default connection test mismatch")
}

func (s *NewBrokerSuite) TestNewBroker_Valid() {
	testCases := []struct {
		name string
		cfg  kafka.Config
	}{
		{
			name: "mixed mode with group id",
			cfg: kafka.Config{
				Mode:       kafka.ModeMixed,
				DSN:        testDSN,
				GroupID:    testGroupID,
				MaxBytes:   testMaxBytes,
				AutoCommit: true,
			},
		},
		{
			name: "producer mode without group id",
			cfg: kafka.Config{
				DSN:      testDSN,
				Mode:     kafka.ModeProducer,
				MaxBytes: testMaxBytes,
				// GroupID intentionally omitted
				AutoCommit: true,
			},
		},
		{
			name: "consumer mode with group id",
			cfg: kafka.Config{
				DSN:        testDSN,
				GroupID:    testGroupID,
				MaxBytes:   testMaxBytes,
				Mode:       kafka.ModeConsumer,
				AutoCommit: true,
			},
		},
		{
			name: "all options",
			cfg: kafka.Config{
				DSN:               testDSN,
				GroupID:           testGroupID,
				MaxBytes:          testMaxBytes,
				AutoCommit:        true,
				ConnectionTest:    false,
				Mode:              kafka.ModeMixed,
				ControllerFactory: nil,
			},
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			broker, err := kafka.NewBroker(&tC.cfg)
			s.Require().NoError(err, "expected nil but got error")
			s.Require().NotNil(broker, "expected broker but got nil")
		})
	}
}

func (s *NewBrokerSuite) TestNewBroker_Fail() {
	testCases := []struct {
		name string
		cfg  *kafka.Config
	}{
		{
			name: "missing config",
			cfg:  nil,
		},
		{
			name: "missing dsn",
			cfg: &kafka.Config{
				Mode:     kafka.ModeMixed,
				GroupID:  testGroupID,
				MaxBytes: testMaxBytes,
			},
		},
		{
			name: "missing group id in mixed mode",
			cfg: &kafka.Config{
				DSN:        testDSN,
				Mode:       kafka.ModeMixed,
				MaxBytes:   testMaxBytes,
				AutoCommit: true,
			},
		},
		{
			name: "missing group id in consumer mode",
			cfg: &kafka.Config{
				DSN:        testDSN,
				Mode:       kafka.ModeConsumer,
				MaxBytes:   testMaxBytes,
				AutoCommit: true,
			},
		},
		{
			name: "max bytes == 0",
			cfg: &kafka.Config{
				Mode:       kafka.ModeMixed,
				DSN:        testDSN,
				GroupID:    testGroupID,
				MaxBytes:   0,
				AutoCommit: true,
			},
		},
		{
			name: "max bytes negative",
			cfg: &kafka.Config{
				Mode:       kafka.ModeMixed,
				DSN:        testDSN,
				GroupID:    testGroupID,
				MaxBytes:   -100,
				AutoCommit: true,
			},
		},
		{
			name: "max bytes reaches Sarama request limit",
			cfg: &kafka.Config{
				Mode:       kafka.ModeProducer,
				DSN:        testDSN,
				MaxBytes:   int(sarama.MaxRequestSize),
				AutoCommit: true,
			},
		},
		{
			name: "batch bytes negative",
			cfg: &kafka.Config{
				Mode:       kafka.ModeProducer,
				DSN:        testDSN,
				MaxBytes:   testMaxBytes,
				BatchBytes: -1,
				AutoCommit: true,
			},
		},
		{
			name: "batch bytes reaches Sarama request limit",
			cfg: &kafka.Config{
				Mode:       kafka.ModeProducer,
				DSN:        testDSN,
				MaxBytes:   testMaxBytes,
				BatchBytes: int64(sarama.MaxRequestSize),
				AutoCommit: true,
			},
		},
		{
			name: "topic create attempts negative",
			cfg: &kafka.Config{
				Mode:                kafka.ModeProducer,
				DSN:                 testDSN,
				MaxBytes:            testMaxBytes,
				TopicCreateAttempts: -1,
			},
		},
		{
			name: "nack backoff negative",
			cfg: &kafka.Config{
				Mode:        kafka.ModeConsumer,
				DSN:         testDSN,
				GroupID:     testGroupID,
				MaxBytes:    testMaxBytes,
				NackBackoff: -time.Second,
			},
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			broker, err := kafka.NewBroker(tC.cfg)
			s.Require().Error(err, "expected error but got nil")
			s.Require().Nil(broker, "expected nil but got broker")
		})
	}
}

func (s *ConnectSuite) TestConnect_Valid() {
	testCases := []struct {
		name string
	}{
		{
			name: "not connected",
		},
		{
			name: "already connected",
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			s.mockFactory.On("New", mock.Anything).
				Return(s.mockCtrl, nil).Once()

			ctx := context.Background()
			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			require.NotNil(s.T(), broker, "expected not nil broker but got nil")

			err = broker.Connect(ctx)
			s.Require().NoError(err, "broker connection failed")

			if tC.name == "already connected" {
				err = broker.Connect(ctx)
				s.Require().NoError(err, "consecutive broker connection failed")
			}
		})
	}
}

func (s *ConnectSuite) TestConnect_Fail() {
	testCases := []struct {
		name       string
		factoryErr error
	}{
		{
			name:       "factory error",
			factoryErr: errors.New("failed to create controller"),
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			s.mockFactory.On("New", mock.Anything).
				Return(nil, tC.factoryErr).Once()

			ctx := context.Background()
			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "expected nil but got error")
			s.Require().NotNil(broker, "expected not-nil broker but got nil")

			err = broker.Connect(ctx)
			s.Require().Error(err, "expected error but got nil")
		})
	}
}

func (s *ConnectSuite) TestConnect_TopicCreationDisabled() {
	s.mockFactory.On("New", mock.MatchedBy(func(cfg kafka.Config) bool {
		return cfg.TopicCreateAttempts == 0
	})).
		Return(s.mockCtrl, nil).Once()

	cfg := kafka.Config{
		Mode:                kafka.ModeProducer,
		DSN:                 testDSN,
		MaxBytes:            testMaxBytes,
		ControllerFactory:   s.mockFactory,
		TopicCreateAttempts: 0,
	}

	broker, err := kafka.NewBroker(&cfg)
	s.Require().NoError(err, "new broker")
	s.Require().NotNil(broker, "broker is nil")

	err = broker.Connect(s.T().Context())
	s.Require().NoError(err, "connect with topic creation disabled")
}

func (s *CloseSuite) TestClose() {
	testCases := []struct {
		name            string
		connect         bool
		subscribe       bool
		expectSubCancel bool
	}{
		{
			name: "not connected",
		},
		{
			name:    "connected no subs",
			connect: true,
		},
		{
			name:            "connected with subs",
			connect:         true,
			subscribe:       true,
			expectSubCancel: true,
		},
		{
			name:    "consecutive calls",
			connect: true,
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			var done <-chan struct{}

			if tC.connect {
				s.mockFactory.On("New", mock.Anything).
					Return(s.mockCtrl, nil).Once()
				// Close releases the producer, and only on the first call:
				// afterwards the broker has no controller left.
				s.mockCtrl.On("Close").Return(nil).Once()
			}

			if tC.subscribe {
				msgsChan := make(chan kafka.AcknowledgeableBrokerMessage)
				s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
					Return(s.mockSub, nil).Once()
				s.mockSub.On("MessagesChannel").Return((<-chan kafka.AcknowledgeableBrokerMessage)(msgsChan)).Maybe()
			}

			if tC.expectSubCancel {
				s.mockSub.On("Cancel", mock.Anything).Return().Once()
			}

			ctx := context.Background()
			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			s.Require().NotNil(broker, "broker is nil")

			if tC.connect {
				err = broker.Connect(ctx)
				s.Require().NoError(err, "connect failed")
			}

			if tC.subscribe {
				done, err = broker.Subscribe(
					ctx,
					"test-topic",
					successfulHandler(func(_ context.Context, _ any, _ func(any, any) error) {}),
				)
				s.Require().NoError(err, "subscribe failed")
				s.Require().NotNil(done, "done channel is nil")
			}

			broker.Close(ctx)
			if tC.subscribe {
				<-done
			}

			if tC.name == "consecutive calls" {
				broker.Close(ctx)
			}
		})
	}
}

// endregion
