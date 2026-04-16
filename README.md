# MIT 6.5840 MapReduce Lab (Lab 1)

Учебный проект по распределённым системам: реализация MapReduce из курса **MIT 6.5840 (Distributed Systems)**.

## Что реализовано

- `Coordinator`: выдача map/reduce задач, переключение фаз, учёт статусов, переназначение задач по таймауту.
- `Worker`: запрос задач по RPC, выполнение map/reduce, запись промежуточных и финальных файлов, отчёт о завершении.
- RPC-контракт между coordinator и worker.

Основная логика находится в:

- `src/mr/coordinator.go`
- `src/mr/worker.go`
- `src/mr/rpc.go`

## Запуск

Из директории `src`:

```bash
make mr
```

## Testing

Локально:

```bash
cd src
make mr
```

В GitHub Actions автоматически запускается тот же набор тестов (`make mr`) на каждый `push` и `pull_request` в ветку `master`.

## Attribution

This repository is based on the MIT 6.5840 lab framework and adapted as a personal educational project.

## Source Note

This lab implementation is conceptually based on the MapReduce model from:

- Jeffrey Dean, Sanjay Ghemawat, **"MapReduce: Simplified Data Processing on Large Clusters"**, OSDI 2004.
  https://research.google/pubs/mapreduce-simplified-data-processing-on-large-clusters/
