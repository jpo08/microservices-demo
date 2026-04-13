# Taller 1: Construcción de Pipelines en Cloud

## Microservices Demo - Votación Tacos vs Burritos



# 1. Introducción 

El presente taller implementa un pipeline de DevOps completo para la aplicación Microservices Demo, una plataforma de votación que permite a los usuarios elegir entre Tacos y Burritos.

La aplicación está compuesta por cuatro microservicios:

* Frontend de votación en Java (Spring Boot)
* Broker Kafka
* Worker en Go
* Aplicación de resultados en Node.js


---

# 2. Estrategias de Branching

## revisar branch-strategy.md

# 3. Patrones de Diseño

## 3.1 External Configuration Store

### Problema

Configuración hardcodeada:

```java
configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
```

### Solución

| Capa       | Mecanismo            |
| ---------- | -------------------- |
| Kubernetes | ConfigMap + Secret   |
| CI/CD      | GitHub Variables     |
| App        | Variables de entorno |

### Código ejemplo (Go)

```go
func getEnv(key, defaultVal string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return defaultVal
}
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: microservices-config
data:
  KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  POSTGRES_HOST: "postgresql"
  WORKER_MAX_RETRIES: "5"
```

### Beneficios

* Misma imagen en todos los entornos
* No recompilar para cambios de config
* Uso de Secrets
* Compatible con 12-Factor App

---

## 3.2 Retry

### Problema

Fallas en servicios externos (Kafka, DB)

### Implementación (Go)

```go
func WithRetry(cfg RetryConfig, operationName string, fn func() error) error {
    for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
        err := fn()
        if err == nil { return nil }

        delay := time.Duration(
            float64(cfg.BaseDelay) * math.Pow(cfg.Factor, float64(attempt-1)))

        time.Sleep(delay)
    }
    return fmt.Errorf("fallo")
}
```

### Implementación (Node.js)

```js
async function withRetry(fn, opts = {}) {
    for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
        try {
            return await fn();
        } catch (err) {
            await new Promise(r => setTimeout(r, 1000));
        }
    }
}
```

### Configuración

| Parámetro    | ENV                   | Default |
| ------------ | --------------------- | ------- |
| Max intentos | WORKER_MAX_RETRIES    | 5       |
| Delay base   | WORKER_RETRY_DELAY_MS | 1000ms  |

---

# 4. Arquitectura

## 4.1 Servicios

| Servicio   | Tecnología | Puerto |
| ---------- | ---------- | ------ |
| vote       | Java       | 8080   |
| kafka      | Kafka      | 9092   |
| worker     | Go         | -      |
| postgresql | PostgreSQL | 5432   |
| result     | Node.js    | 4000   |

---

## 4.2 Flujo

1. Usuario vota
2. Vote envía a Kafka
3. Worker consume
4. Guarda en PostgreSQL
5. Result muestra en tiempo real

---

## 4.3 CI/CD

| Pipeline | Trigger | Jobs                |
| -------- | ------- | ------------------- |
| vote     | push/PR | build, test, deploy |
| worker   | push/PR | build, test         |
| result   | push/PR | build               |
| infra    | push    | validate, deploy    |

---

# 5. Pipelines de Desarrollo

## 5.1 Vote (Java)

* build & test (Maven)
* code quality (OWASP)
* docker build + Trivy
* deploy staging (Okteto)

---

## 5.2 Worker (Go)

* go vet + staticcheck
* tests con race detection
* integration tests con Kafka y PostgreSQL

---

## 5.3 Result (Node.js)

* npm ci
* tests
* audit
* docker build

---

## 5.4 Smoke Tests

```bash
bash scripts/smoke-test.sh all http://app
```

Verifica:

* HTTP 200
* health checks

---

# 6. Pipeline de Infraestructura

## Validación

```bash
helm lint
helm template
```

## Seguridad

```bash
checkov -d rendered/
```

## Deploy

```bash
helm upgrade --install --atomic
```

## Rollback

```bash
gh workflow run infrastructure-pipeline.yml
```

---

# 7. Infraestructura

## Estructura

```
microservices-demo/
  infrastructure/
  vote/chart/
  result/chart/
  worker/chart/
  config/configmap.yaml
```

## ConfigMap en deployment

```yaml
envFrom:
  - configMapRef:
      name: microservices-config
```

## Despliegue

```bash
okteto deploy
```

---

