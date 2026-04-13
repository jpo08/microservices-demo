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

El patron External Configuration Store (Almacen de Configuracion Externo) consiste en sacar toda la configuracion de la aplicacion fuera del codigo fuente y de las imagenes Docker, almacenandola en un sistema centralizado y gestionado externamente.

### Problema

Configuración hardcodeada en el codigo fuente. Por ejemplo, en KafkaProducerConfig.java:

```java
configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
```
Y en worker/main.go:

```
host     = "postgresql"
password = "okteto"
```
Esto hace imposible reutilizar la misma imagen Docker en distintos ambientes sin reconstruirla.

### Solución

| Capa       | Mecanismo            |
| ---------- | -------------------- |
| Kubernetes | ConfigMap + Secret   |
| CI/CD      | GitHub Actions Variables/Secrets  |
| App        | Variables de entorno |

### Codigo del patron en worker/main.go 

```go
// Lee configuracion desde variables de entorno (inyectadas por K8s ConfigMap)
func getEnv(key, defaultVal string) string {
    if v := os.Getenv(key); v != "" { return v }
    return defaultVal
}

func getPostgresConnString() string {
    host     := getEnv("POSTGRES_HOST", "postgresql")
    password := getEnv("POSTGRES_PASSWORD", "okteto")
}
```

### ConfigMap de Kubernetes 

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: microservices-config
data:
  KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  KAFKA_TOPIC_VOTES: "votes"
  POSTGRES_HOST: "postgresql"
  WORKER_MAX_RETRIES: "5"

```

### Beneficios

* La misma imagen Docker funciona en dev, staging y produccion.
* Cambiar el servidor de Kafka no requiere reconstruir ninguna imagen.
* Las credenciales de BD se gestionan en Kubernetes Secrets (cifrados).
* El pipeline CI/CD inyecta la configuracion de cada ambiente automaticamente.


---

## 3.2 Retry

El patron Retry hace que una operacion fallida se reintente automaticamente un numero configurable de veces antes de considerar que ha fallado definitivamente. Es esencial en arquitecturas de microservicios donde los servicios externos pueden estar temporalmente no disponibles.

### Problema

Fallas en servicios externos (Kafka, DB)

### Implementación en worker/main.go:

```go
func WithRetry(cfg RetryConfig, operationName string, fn func() error) error {
    for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
        err := fn()
        if err == nil { return nil }

        // Backoff exponencial: baseDelay * factor^(attempt-1)
        delay := time.Duration(
            float64(cfg.BaseDelay) * math.Pow(cfg.Factor, float64(attempt-1)))
        if delay > cfg.MaxDelay { delay = cfg.MaxDelay }

        log.Printf("[RETRY] intento %d/%d fallido. Esperando %v...",
            attempt, cfg.MaxAttempts, delay)
        time.Sleep(delay)
    }
    return fmt.Errorf("operacion fallida despues de %d intentos", cfg.MaxAttempts)
}

```

### Implementación en result/server.js:

```js
async function withRetry(operationName, fn, opts = {}) {
    const maxAttempts = opts.maxAttempts || CONFIG.maxRetries;
    const baseDelay   = opts.baseDelay   || CONFIG.retryDelayMs;
    let lastError;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try { return await fn(); } catch (err) {
            lastError = err;
            const delay = Math.min(baseDelay * Math.pow(2, attempt-1), 30000);
            await new Promise(r => setTimeout(r, delay));
        }

```

### Configuración

| Parámetro    | ENV                   | Default |
| ------------ | --------------------- | ------- |
| Max intentos | WORKER_MAX_RETRIES    | 5       |
| Delay base   | WORKER_RETRY_DELAY_MS | 1000ms  |
| Delay maximo | -                     | 30 s    |

---

# 4. Arquitectura

## 4.1 Servicios
La aplicacion sigue una arquitectura de microservicios basada en eventos con los siguientes componentes:

| Servicio   | Tecnología | Puerto |
| ---------- | ---------- | ------ |
| vote       | Java       | 8080   |
| kafka      | Kafka      | 9092   |
| worker     | Go         | -      |
| postgresql | PostgreSQL | 5432   |
| result     | Node.js    | 4000   |

---

## 4.2 Flujo de datos

1. El usuario accede a vote (Java, :8080) y hace clic en Tacos o Burritos.
2. Vote publica un mensaje en el topic "votes" de Kafka (con RETRY si Kafka no responde).
3. El worker (Go) consume el mensaje de Kafka (con RETRY en la conexion inicial).
4. El worker inserta/actualiza el voto en PostgreSQL (con RETRY si la DB falla).
5. result (Node.js) consulta PostgreSQL cada segundo y emite los datos via Socket.io.
6. El navegador del usuario recibe las actualizaciones en tiempo real.


---

## 4.3 Arquitectura de CI/CD

| Pipeline                      | Trigger                                 | Jobs                                                        |
|------------------------------|------------------------------------------|-------------------------------------------------------------|
| vote-pipeline.yml            | push/PR a vote/**                        | build-test > code-quality > docker-build > deploy-staging   |
| worker-pipeline.yml          | push/PR a worker/**                      | build-test > docker-build > integration-test                |
| result-pipeline.yml          | push/PR a result/**                      | build-test > docker-build                                  |
| infrastructure-pipeline.yml  | push/PR a infra/** o charts              | validate-helm > security-scan > deploy-staging > deploy-prod |

---

# 5. Pipelines de Desarrollo

## 5.1  Pipeline del Servicio Vote (Java / Spring Boot)
**Archivo:** `.github/workflows/vote-pipeline.yml`

### Job 1: build-and-test
- Configura JDK 22 con Eclipse Temurin.
- Aplica el External Configuration Store: sobreescribe `application.properties` con valores de GitHub Variables (`vars.KAFKA_BOOTSTRAP_SERVERS`, `vars.KAFKA_TOPIC`).
- Ejecuta `mvn clean package -DskipTests` para compilar.
- Ejecuta `mvn test` para correr las pruebas unitarias.
- Sube los reportes de Surefire como artefacto.

### Job 2: code-quality
- Ejecuta OWASP Dependency Check (falla si CVSS >= 9).
- Genera reporte HTML de vulnerabilidades de dependencias.

### Job 3: docker-build
- Construye imagen Docker con cache de GitHub Actions (`type=gha`).
- Aplica tags: rama, SHA del commit, `"latest"` solo en `main`.
- Hace push a Docker Hub.
- Escanea la imagen con Trivy (CRITICAL, HIGH).

### Job 4: deploy-staging
- Se ejecuta solo en pushes a `develop`.
- Usa el entorno `"staging"` de GitHub (requiere aprobación si se configura).
- Despliega con `okteto deploy --wait`.
- Ejecuta `scripts/smoke-test.sh` para verificar que el servicio responde.

---

## 5.2 Pipeline del Servicio Worker (Go)
**Archivo:** `.github/workflows/worker-pipeline.yml`

### Job 1: build-and-test
- Configura Go 1.24 con cache de `go.mod`.
- Verifica dependencias con `go mod verify`.
- Ejecuta `go vet` y `staticcheck` para análisis estático.
- Corre tests con `-race` (detector de race conditions) y genera coverage.
- Compila el binario con `CGO_ENABLED=0` para contenedor `scratch` (imagen mínima).

### Job 2: integration-test (solo en PRs)
- Levanta servicios reales de Kafka y PostgreSQL como containers de GitHub Actions.
- Espera que Kafka esté disponible con `scripts/wait-for-kafka.sh`.
- Ejecuta tests de integración que verifican el flujo completo.

---

## 5.3 Pipeline del Servicio Result (Node.js)
**Archivo:** `.github/workflows/result-pipeline.yml`

- Configura Node.js 20 con cache de `npm`.
- Ejecuta `npm ci` (instalación determinista de dependencias).
- Corre linter y tests unitarios.
- Ejecuta `npm audit` para detectar vulnerabilidades críticas.
- Construye y sube imagen Docker a Docker Hub.

---

## 5.4 Script de Smoke Tests

El script `scripts/smoke-test.sh` verifica que los servicios responden correctamente después del despliegue:

```bash
# Uso: bash scripts/smoke-test.sh <servicio> <url-base>
bash scripts/smoke-test.sh all http://mi-app.staging.okteto.net

# El script verifica:
# - HTTP 200 en el endpoint principal de vote y result
# - /health endpoint del servicio result
# - Espera con timeout configurable hasta que el servicio esté listo

```
# 6. Pipeline de Infraestructura  
**Archivo:** `.github/workflows/infrastructure-pipeline.yml`

## 6.1 Validación de Helm Charts  
El primer job valida todos los Helm charts del proyecto:

```bash
helm lint infrastructure/
helm lint vote/chart/
helm lint result/chart/
helm lint worker/chart/
helm template vote vote/chart/ --values vote/chart/values.yaml > /dev/null
````

Si cualquier chart falla la validación, el pipeline se detiene inmediatamente.

---

## 6.2 Escaneo de Seguridad en Manifiestos

Se renderizan los templates de Helm y se escanean con Checkov:

```bash
helm template infrastructure infrastructure/ > rendered/infrastructure.yaml
checkov -d rendered/ --framework kubernetes --soft-fail
```

Checkov verifica buenas prácticas de seguridad en Kubernetes: usuarios no-root, límites de recursos, read-only filesystem, etc.

---

## 6.3 Despliegue de Infraestructura (Staging y Producción)

El despliegue de Kafka y PostgreSQL se hace vía Helm con la bandera `--atomic`, que hace rollback automático si el despliegue falla:

```bash
helm upgrade --install infrastructure infrastructure/ \
  --namespace $NAMESPACE \
  --wait \
  --timeout 5m \
  --atomic   # rollback automático si falla
```

---

## 6.4 Rollback Manual

El pipeline permite ejecutar un rollback manual mediante `workflow_dispatch`:

```bash
# Desde la UI de GitHub Actions o con gh CLI:
gh workflow run infrastructure-pipeline.yml \
  -f environment=staging \
  -f action=rollback
```

Esto ejecuta `scripts/rollback.sh` que usa `helm rollback` para volver a la versión anterior de cada release.

---

# 7. Implementación de la Infraestructura  

## 7.1 Estructura de Helm Charts  
El proyecto usa Helm como gestor de paquetes de Kubernetes. Cada microservicio tiene su propio chart en su directorio:

```plaintext
microservices-demo/
  infrastructure/     # Chart de Kafka + PostgreSQL
  vote/chart/         # Chart del servicio Java
  result/chart/       # Chart del servicio Node.js
  worker/chart/       # Chart del servicio Go
  config/
    configmap.yaml    # External Configuration Store (K8s ConfigMap)
````

---

## 7.2 ConfigMap de Kubernetes (External Configuration Store)

El archivo `config/configmap.yaml` centraliza toda la configuración de la aplicación. Los deployments de Kubernetes referencian este ConfigMap para inyectar variables de entorno en los pods:

```yaml
# En el deployment de worker (worker/chart/templates/deployment.yaml):
envFrom:
  - configMapRef:
      name: microservices-config
  - secretRef:
      name: microservices-secrets
```

---

## 7.3 Despliegue con Okteto

Para el despliegue en Okteto, se usa el archivo `okteto.yml` que orquesta la construcción de imágenes y el despliegue de los charts:

```bash
$ git clone https://github.com/okteto/microservices-demo
$ cd microservices-demo
$ kubectl apply -f config/configmap.yaml  # Aplicar External Config Store
$ okteto login
$ okteto deploy
```

---

## 7.4 Secrets en CI/CD

Las credenciales sensibles se gestionan como GitHub Secrets y nunca aparecen en el código:

| Secret                   | Scope       | Uso                                      |
| ------------------------ | ----------- | ---------------------------------------- |
| DOCKER_USERNAME          | Repo        | Login a Docker Hub para push de imágenes |
| DOCKER_PASSWORD          | Repo        | Login a Docker Hub                       |
| OKTETO_TOKEN             | Env         | Autenticación con plataforma Okteto      |
| OKTETO_URL               | Env         | URL de la instancia Okteto               |
| OKTETO_NAMESPACE_STAGING | Env:staging | Namespace de staging en K8s              |
| OKTETO_NAMESPACE_PROD    | Env:prod    | Namespace de producción en K8s           |



---

