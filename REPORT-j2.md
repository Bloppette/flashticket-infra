# Rapport TD2 — prise de commande asynchrone

## Schéma du flux

```text
Client -> create-order (HTTP 202) -> SQS flashticket-orders
       -> process-order (lot de 10)
       -> DynamoDB orders
       -> Kinesis flashticket-sales (OrderConfirmed)

Échec après 3 réceptions : SQS flashticket-orders-dlq
```

## Mesures

À compléter après `make up`, `make bootstrap`, `make deploy`, `make smoke` et `make load`.

| Mesure | Résultat |
|---|---|
| Requêtes 202 k6 | |
| Messages traités | |
| Lignes DynamoDB `orders` | |
| Messages DLQ | |
| Durée du premier appel | |
| Durée du second appel | |

## Tests

- API valide/invalide : 
- Doublon avec 5 requêtes : 
- Message empoisonné, tentatives/DLQ : 
- Échec partiel : 

## Plantage après écriture de `orders`

La réservation d'idempotence reste à l'état `IN_PROGRESS` avec les étapes `RESERVED` puis `ORDER_WRITTEN`. Si `process-order` plante après l'écriture de la commande mais avant `DONE`, le replay retrouve la clé en cours et vérifie la ligne `orders` avant de poursuivre. Il peut donc reprendre la publication Kinesis et terminer par `DONE` sans écrire une seconde ligne. Un nouveau message avec la même clé à l'état `DONE` est ignoré.
