# TD 2 — La prise de commande asynchrone

**Infrastructure : Cloud, Linux et serverless · Jour 2 · 13h30–16h30**

Sujet à suivre en binôme : API, file SQS, consommateur idempotent, test de charge.

---

## Mission et planning

### Un flux de commande qui encaisse le pic

**Mission :** construire le flux API → `create-order` → file SQS → `process-order` → table des commandes → événement `OrderConfirmed`, le soumettre à un pic de charge et prouver qu’aucune commande n’est perdue ni traitée deux fois.

**Fournitures :**
- `docker-compose.yml` avec LocalStack ;
- déploiement des fonctions (`make deploy`) ;
- squelette de deux fonctions (Node.js/TypeScript et Python) ;
- script de charge k6 ;
- branche `solution-j1` pour les binômes en retard.

Les ressources sont créées en ligne de commande aujourd’hui ; elles seront reprises en Terraform au jour 3.

### Planning

| Heure | Activité | Durée |
|---|---|---:|
| 13h30 | Briefing | 10 min |
| 13h40 | Partie A | 1 h |
| 14h40 | Pause | 10 min |
| 14h50 | Partie B | 50 min |
| 15h40 | Partie C | 50 min |
| 16h30 | Fin du TD | — |

---

## Partie A — 1 h (1/2) : LocalStack et ressources

1. Démarrer LocalStack : `make up`, charger les variables avec `source .env.example`, puis vérifier `aws sts get-caller-identity`.
2. Créer la DLQ, la file principale avec sa politique de reprise, les tables `orders` et `idempotency` (TTL sur le champ `ttl`) et le stream `flashticket-sales`.

**Réglages :** visibilité 60 s, `maxReceiveCount` 3, facturation `PAY_PER_REQUEST`, 1 shard.

### Extrait de `scripts/bootstrap.sh`

```bash
aws sqs create-queue \
  --queue-name flashticket-orders-dlq

aws sqs create-queue \
  --queue-name flashticket-orders \
  --attributes VisibilityTimeout=60,RedrivePolicy=maxReceiveCount 3

aws dynamodb create-table \
  --table-name orders ...

aws dynamodb create-table \
  --table-name idempotency ...

aws kinesis create-stream \
  --stream-name flashticket-sales \
  --shard-count 1
```

---

## Partie A — étape 2 : files

### Files : DLQ et reprise après 3 essais

```bash
DLQ_URL=$(aws sqs create-queue \
  --queue-name flashticket-orders-dlq \
  --query QueueUrl --output text)

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)

REDRIVE="{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}"
ATTRS="{\"VisibilityTimeout\":\"60\",\"RedrivePolicy\":\"$REDRIVE\"}"

aws sqs create-queue --queue-name flashticket-orders \
  --attributes "$ATTRS"
```

**À retenir :**
- **Visibilité 60 s :** un message en cours de traitement reste invisible 60 s.
- **`maxReceiveCount` 3 :** après 3 réceptions sans suppression, SQS le déplace dans la DLQ.

**Vérifier :**

```bash
aws sqs get-queue-attributes --queue-url … --attribute-names All
```

---

## Partie A — étape 2 : tables et flux

### Tables, TTL et flux d’événements

```bash
aws dynamodb create-table --table-name orders \
  --attribute-definitions AttributeName=orderId,AttributeType=S \
  --key-schema AttributeName=orderId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb create-table --table-name idempotency \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live --table-name idempotency \
  --time-to-live-specification Enabled=true,AttributeName=ttl

aws kinesis create-stream \
  --stream-name flashticket-sales --shard-count 1
```

**Structure des tables :**
- `orders` : clé `orderId`.
- `idempotency` : clé `pk` (l’`Idempotency-Key` du client), champ `ttl` pour l’expiration.

Mettez ces commandes dans `scripts/bootstrap.sh`, relançable après `make reset`.

---

## Partie A — 1 h (2/2) : la fonction `create-order`

3. Compléter `create-order` :
   - valider le corps (identifiant d’événement, places, e-mail) ;
   - exiger l’en-tête `Idempotency-Key` ;
   - publier dans la file ;
   - répondre `202` avec l’identifiant de corrélation.

4. Déployer la fonction (archive zip puis `aws lambda create-function`) et l’exposer avec :

```bash
aws lambda create-function-url-config \
  --function-name create-order --auth-type NONE
```

**Local uniquement :** noter dans le rapport ce qu’il faudrait en production (autorisation, limitation de débit).

5. Tester avec `curl`, puis lire le message avec `aws sqs receive-message`.

---

## Partie A — étape 4 : déployer `create-order` et l’exposer

```bash
cd functions/create-order && npm ci && npm run package && cd ../..

QUEUE_URL=$(aws sqs get-queue-url \
  --queue-name flashticket-orders --query QueueUrl --output text)

aws lambda create-function --function-name create-order \
  --runtime nodejs20.x --handler index.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --zip-file fileb://functions/create-order/create-order.zip \
  --timeout 10 --environment "Variables={QUEUE_URL=$QUEUE_URL}"

aws lambda create-function-url-config \
  --function-name create-order --auth-type NONE
```

**Remarques :**
- La dernière commande affiche la `FunctionUrl` : c’est l’adresse à appeler avec `curl`.
- Raccourci fourni : `make deploy` construit et déploie les deux fonctions, et se relance après chaque modification du code (il met à jour au lieu de recréer).
- Voir la file :

```bash
aws sqs receive-message --queue-url "$QUEUE_URL"
```

---

## Partie B — 50 min (1/2) : le consommateur `process-order`

1. Écrire `process-order` : pour chaque message du lot :
   - réserver la clé d’idempotence (écriture conditionnelle) ;
   - enregistrer la commande ;
   - publier `OrderConfirmed` dans le stream ;
   - puis marquer la clé `DONE`.

2. Lier la file à la fonction avec `create-event-source-mapping` : `batch-size` 10 et `ReportBatchItemFailures`.

Renvoyer la liste des messages en échec pour ne pas rejouer tout le lot.

### Réservation atomique de la clé

```text
PutItem {
  TableName: "idempotency",
  Item: { pk, status: IN_PROGRESS, ttl: now + 86400 },
  ConditionExpression: "attribute_not_exists(pk)"
}

// ConditionalCheckFailed => doublon,
// on ne retraite pas
```

---

## Partie B — étape 2 : déployer `process-order` et le relier à la file

```bash
(cd functions/process-order && zip -q -j ../process-order.zip handler.py)

aws lambda create-function --function-name process-order \
  --runtime python3.12 --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --zip-file fileb://functions/process-order.zip --timeout 10

QUEUE_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)

aws lambda create-event-source-mapping \
  --function-name process-order --event-source-arn "$QUEUE_ARN" \
  --batch-size 10 --function-response-types ReportBatchItemFailures
```

**Remarques :**
- Les noms des tables et du flux ont des valeurs par défaut (`orders`, `idempotency`, `flashticket-sales`) : rien à passer si vous gardez ces noms.
- Le lien file → fonction se vérifie avec :

```bash
aws lambda list-event-source-mappings
```

- Tout ceci est fait par `make deploy` (relançable).
- Tests prêts à l’emploi : `make smoke` (202/400, doublon, DLQ).

---

## Partie B — 50 min (2/2) : trois tests à réussir

### 3. Doublon
Envoyer 5 fois la même requête avec la même `Idempotency-Key`.

**Attendu :** une seule ligne dans `orders`.

### 4. Message empoisonné
Envoyer directement dans la file un message JSON invalide.

**Attendu :** 3 tentatives puis la DLQ. Relever le temps et expliquer le rôle de la durée de visibilité.

### 5. Échec partiel
Faire échouer un message sur deux dans un lot de dix.

**Attendu :** les autres ne sont pas retraités.

---

## Partie C — 50 min : charge, cold start et observabilité

1. Lancer le script k6 : 50 requêtes par seconde pendant 60 s (3 000 commandes), chacune avec une clé unique.
2. Compter les requêtes acceptées (`202`), les messages traités, les lignes dans `orders` et les messages en DLQ. Les trois premiers nombres doivent être égaux, la DLQ vide.
3. Mesurer le démarrage à froid : durée de la première invocation après 10 min d’inactivité, comparée à la suivante.
4. Retrouver une commande de bout en bout grâce à son `correlationId` : `aws logs filter-log-events`.
5. Rédiger `REPORT-j2.md` : schéma du flux, résultats des trois tests, réponse à « que se passe-t-il si `process-order` plante après avoir écrit la commande mais avant de marquer la clé `DONE` ? »

---

## Partie C — étapes 1 à 4 : commandes de mesure et de traçage

### 1. Charge (URL = FunctionUrl de `create-order`)

```bash
k6 run -e URL="$URL" loadtest/k6.js
```

### 2. Compteurs

```bash
aws dynamodb scan --table-name orders --select COUNT --query Count

aws sqs get-queue-attributes --queue-url "$DLQ_URL" \
  --attribute-names ApproximateNumberOfMessages

aws logs filter-log-events --log-group-name /aws/lambda/process-order \
  --filter-pattern '"commande traitée"' --query 'length(events)'
```

**Informations utiles :**
- **202 acceptés :** dans le résumé k6, compteur `orders_accepted_202` (aussi dans `loadtest/summary.json`).
- **Cold start :** chronométrer un premier appel `curl` après 10 min d’inactivité, puis un second appel.
- Le `correlationId` est dans la réponse `202` et dans l’en-tête `x-correlation-id`.

### 4. Suivre une commande de bout en bout

```bash
aws logs filter-log-events --log-group-name /aws/lambda/create-order \
  --filter-pattern '"<correlationId>"'

aws logs filter-log-events --log-group-name /aws/lambda/process-order \
  --filter-pattern '"<correlationId>"'
```

---

## Livrables et barème

### Ce qui est relu au point de contrôle

- `functions/create-order/` et `functions/process-order/` avec leurs tests.
- `scripts/bootstrap.sh` (création des ressources) et `loadtest/k6.js`.
- `REPORT-j2.md` avec les compteurs de la partie C.

### Barème : 15 points

| Critère | Points |
|---|---:|
| API de commande (202 / 400) | 2 |
| Aucune perte sous charge | 3 |
| Idempotence (5 envois, 1 commande) | 3 |
| Reprise et DLQ (3 tentatives) | 2 |
| Échecs partiels | 2 |
| Journaux corrélés et analyse du plantage | 3 |
| **Total** | **15** |

---

## Pièges fréquents : quatre erreurs à éviter

| Visibilité | Lot en erreur | `DONE` trop tôt | TTL oublié |
|---|---|---|---|
| Durée plus courte que la durée maximale de la fonction : les messages réapparaissent pendant leur traitement. Viser au moins 6 fois la durée maximale. | Réponse en erreur pour tout le lot à cause d’un seul message : renvoyer les éléments en échec. | Clé marquée `DONE` avant la fin du traitement : un plantage fait perdre la commande. | Pas de TTL sur la table d’idempotence : elle grossit sans limite. |
