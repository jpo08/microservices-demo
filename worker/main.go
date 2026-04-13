package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"time"

	_ "github.com/lib/pq"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/IBM/sarama"
)

// ============================================================
// RETRY PATTERN - Implementación
// ============================================================
// El patrón Retry permite que el worker reintente operaciones
// fallidas (conexión a Kafka, PostgreSQL e inserción de votos)
// con backoff exponencial y jitter para evitar tormentas de
// reintentos cuando múltiples instancias fallan al mismo tiempo.
//
// Características implementadas:
//   - Backoff exponencial: espera 2^intento * baseDelay
//   - Jitter: variación aleatoria para evitar sincronización
//   - Máximo de reintentos configurable via ENV
//   - Logging de cada intento para observabilidad
// ============================================================

var (
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default(getEnv("KAFKA_BROKER_LIST", "kafka:9092")).Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default(getEnv("KAFKA_TOPIC", "votes")).String()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
)

// RetryConfig contiene la configuración del patrón Retry
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Factor      float64 // multiplicador exponencial
}

// DefaultRetryConfig retorna la configuración por defecto leída desde ENV
// (External Configuration Store Pattern aplicado aquí también)
func DefaultRetryConfig() RetryConfig {
	maxRetries := 5
	if v := os.Getenv("WORKER_MAX_RETRIES"); v != "" {
		fmt.Sscanf(v, "%d", &maxRetries)
	}
	baseDelayMs := 1000
	if v := os.Getenv("WORKER_RETRY_DELAY_MS"); v != "" {
		fmt.Sscanf(v, "%d", &baseDelayMs)
	}
	return RetryConfig{
		MaxAttempts: maxRetries,
		BaseDelay:   time.Duration(baseDelayMs) * time.Millisecond,
		MaxDelay:    30 * time.Second,
		Factor:      2.0,
	}
}

// WithRetry ejecuta la función fn con la estrategia de Retry configurada.
// Implementa backoff exponencial con cap en MaxDelay.
func WithRetry(cfg RetryConfig, operationName string, fn func() error) error {
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err := fn()
		if err == nil {
			if attempt > 1 {
				log.Printf("[RETRY] '%s' exitoso en el intento %d/%d", operationName, attempt, cfg.MaxAttempts)
			}
			return nil
		}

		lastErr = err
		if attempt == cfg.MaxAttempts {
			break
		}

		// Backoff exponencial: baseDelay * factor^(attempt-1), con cap
		delay := time.Duration(float64(cfg.BaseDelay) * math.Pow(cfg.Factor, float64(attempt-1)))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		log.Printf("[RETRY] '%s' falló (intento %d/%d): %v. Reintentando en %v...",
			operationName, attempt, cfg.MaxAttempts, err, delay)
		time.Sleep(delay)
	}

	return fmt.Errorf("operación '%s' falló después de %d intentos: %w", operationName, cfg.MaxAttempts, lastErr)
}

// ── External Configuration Store: leer configuración de ENV ──
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getPostgresConnString() string {
	host := getEnv("POSTGRES_HOST", "postgresql")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "okteto")
	password := getEnv("POSTGRES_PASSWORD", "okteto")
	dbname := getEnv("POSTGRES_DB", "votes")
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

// openDatabaseWithRetry abre la conexión a PostgreSQL aplicando el Retry Pattern
func openDatabaseWithRetry(retryCfg RetryConfig) *sql.DB {
	var db *sql.DB
	err := WithRetry(retryCfg, "abrir conexión a PostgreSQL", func() error {
		var e error
		db, e = sql.Open("postgres", getPostgresConnString())
		if e != nil {
			return e
		}
		return db.Ping()
	})
	if err != nil {
		log.Fatalf("[FATAL] No se pudo conectar a la base de datos: %v", err)
	}
	log.Println("[DB] Conexión a PostgreSQL establecida exitosamente")
	return db
}

// getKafkaMasterWithRetry crea el consumidor Kafka aplicando el Retry Pattern
func getKafkaMasterWithRetry(retryCfg RetryConfig) sarama.Consumer {
	var master sarama.Consumer
	err := WithRetry(retryCfg, "conectar a Kafka", func() error {
		var e error
		master, e = sarama.NewConsumer(*brokerList, nil)
		return e
	})
	if err != nil {
		log.Fatalf("[FATAL] No se pudo conectar a Kafka: %v", err)
	}
	log.Printf("[KAFKA] Conexión establecida con brokers: %v", *brokerList)
	return master
}

// insertVoteWithRetry inserta un voto en PostgreSQL aplicando el Retry Pattern
func insertVoteWithRetry(db *sql.DB, retryCfg RetryConfig, userID, vote string) error {
	stmt := `INSERT INTO "votes"("id", "vote") VALUES($1, $2) ON CONFLICT(id) DO UPDATE SET vote = $2`
	return WithRetry(retryCfg, "insertar voto en DB", func() error {
		_, err := db.Exec(stmt, userID, vote)
		return err
	})
}

func main() {
	kingpin.Parse()

	retryCfg := DefaultRetryConfig()
	log.Printf("[CONFIG] Retry configurado: maxAttempts=%d, baseDelay=%v, maxDelay=%v",
		retryCfg.MaxAttempts, retryCfg.BaseDelay, retryCfg.MaxDelay)

	// Conectar a PostgreSQL con Retry
	db := openDatabaseWithRetry(retryCfg)
	defer db.Close()

	// Crear tabla si no existe
	createTableStmt := `CREATE TABLE IF NOT EXISTS votes (id VARCHAR(255) NOT NULL UNIQUE, vote VARCHAR(255) NOT NULL)`
	if err := WithRetry(retryCfg, "crear tabla votes", func() error {
		_, e := db.Exec(createTableStmt)
		return e
	}); err != nil {
		log.Fatalf("[FATAL] No se pudo crear la tabla: %v", err)
	}

	// Conectar a Kafka con Retry
	master := getKafkaMasterWithRetry(retryCfg)
	defer master.Close()

	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("[FATAL] No se pudo consumir el topic %s: %v", *topic, err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Printf("[KAFKA ERROR] %v", err)
			case msg := <-consumer.Messages():
				*messageCountStart++
				userID := string(msg.Key)
				vote := string(msg.Value)
				log.Printf("[MSG] Recibido voto: user=%s vote=%s", userID, vote)

				// Insertar con Retry Pattern
				if err := insertVoteWithRetry(db, retryCfg, userID, vote); err != nil {
					log.Printf("[ERROR] No se pudo guardar el voto de %s: %v", userID, err)
				}
			case <-signals:
				log.Println("[SIGNAL] Interrupción detectada, cerrando worker...")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	log.Printf("[DONE] Procesados %d mensajes en total", *messageCountStart)
}
