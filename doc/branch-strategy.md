# Estrategia de Branching — Microservices Demo

## Índice
1. [Estrategia para Desarrolladores — GitFlow Adaptado](#1-estrategia-para-desarrolladores)
2. [Estrategia para Operaciones — Trunk-Based Development](#2-estrategia-para-operaciones)
3. [Flujo completo de un feature](#3-flujo-completo-de-un-feature)
4. [Flujo de emergencia (hotfix)](#4-flujo-de-emergencia)

---

## 1. Estrategia para Desarrolladores — GitFlow Adaptado

Se usa **GitFlow adaptado para microservicios**. Cada servicio (vote, worker, result) comparte la misma estrategia de ramas pero sus pipelines se activan de forma independiente gracias a los filtros `paths:` en GitHub Actions.

### Ramas permanentes

| Rama      | Propósito                        | Quién puede hacer push |
|-----------|----------------------------------|------------------------|
| `main`    | Producción estable y etiquetada  | Nadie directo (solo merge de `release/*` o `hotfix/*`) |
| `develop` | Integración continua del equipo  | Nadie directo (solo merge de `feature/*` o `bugfix/*`) |

### Ramas temporales

| Tipo        | Patrón de nombre                              | Base    | Se mergea en              |
|-------------|-----------------------------------------------|---------|---------------------------|
| `feature/*` | `feature/<servicio>/<TICKET>-descripcion`     | develop | develop (squash merge)    |
| `bugfix/*`  | `bugfix/<servicio>/<TICKET>-descripcion`      | develop | develop (squash merge)    |
| `release/*` | `release/v<major>.<minor>.<patch>`            | develop | main + develop (merge commit) |
| `hotfix/*`  | `hotfix/<TICKET>-descripcion`                 | main    | main + develop (merge commit) |

### Ejemplos de nombres de ramas

```bash
feature/vote/US-42-agregar-opcion-enchilada
feature/worker/US-18-validar-votos-duplicados
feature/result/US-30-grafica-tiempo-real
bugfix/vote/BUG-77-cookie-no-persiste
bugfix/result/BUG-88-websocket-desconecta
hotfix/SEC-05-sanitizar-input-kafka
release/v1.2.0
```

---

## 2. Estrategia para Operaciones — Trunk-Based Development

El equipo de operaciones usa **Trunk-Based Development (TBD)** para la infraestructura como código. Se trabaja en ramas de vida muy corta (máximo 2 días) que se integran directamente al trunk de infra.

### Ramas de operaciones

| Rama           | Propósito                                     | Regla de vida      |
|----------------|-----------------------------------------------|--------------------|
| `main-infra`   | Trunk de infraestructura — siempre desplegable | Permanente         |
| `infra/<TICKET>-desc` | Cambios de Terraform, Helm values, K8s YAML | Máximo 2 días |
| `ops/env/staging`     | Overrides de configuración de staging     | Permanente         |
| `ops/env/prod`        | Overrides de configuración de producción  | Permanente         |
| `ops/hotfix/<TICKET>` | Parches urgentes de infraestructura       | Máximo 4 horas     |

### Ejemplos de ramas de operaciones

```bash
infra/INF-12-aumentar-replicas-worker
infra/INF-15-actualizar-kafka-3.8
infra/INF-20-agregar-network-policy
ops/hotfix/INF-99-corregir-secret-pg
```
---

## 3. Flujo Completo de un Feature

```bash
# 1. Crear rama desde develop
git checkout develop
git pull origin develop
git checkout -b feature/vote/US-42-agregar-opcion-enchilada

# 2. Desarrollar y hacer commits
git add vote/src/...
git commit -m "feat(vote): agregar opción Enchilada al formulario [US-42]"
git commit -m "test(vote): agregar tests para nueva opción [US-42]"

# 3. Mantener rama actualizada
git fetch origin
git rebase origin/develop

# 4. Push y abrir Pull Request hacia develop
git push origin feature/vote/US-42-agregar-opcion-enchilada
# → Abrir PR en GitHub: feature/vote/US-42... → develop
# → Pipeline vote-ci.yml se activa automáticamente (solo jobs test + security)
# → Revisor aprueba → squash merge → rama eliminada

# 5. Al merge en develop: pipeline completo
# → jobs: test → security → build-push → deploy-staging → smoke-test

```

---

## 5. Flujo de Emergencia (Hotfix)

```bash
# Hotfix sale SIEMPRE desde main, no desde develop

# 1. Crear rama desde main
git checkout main
git pull origin main
git checkout -b hotfix/SEC-05-sanitizar-input-kafka

# 2. Corregir el problema
git add ...
git commit -m "fix(vote): sanitizar input antes de publicar en Kafka [SEC-05]"

# 3. PR hacia main (con aprobación acelerada del tech lead)
git push origin hotfix/SEC-05-sanitizar-input-kafka
# → Pipeline CI corre tests rápidos
# → Aprobación → merge a main → deploy-production (Blue/Green)
# → Tag: v1.1.1

# 4. OBLIGATORIO: también mergear a develop para no perder el fix
git checkout develop
git merge --no-ff hotfix/SEC-05-sanitizar-input-kafka
git push origin develop
```

