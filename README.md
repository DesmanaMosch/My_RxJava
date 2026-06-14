# Отчёт

## Архитектура системы

```
src/main/java/rx/core/
├── Observer.java            — интерфейс наблюдателя
├── ObservableEmitter.java   — посредник между источником и Observer
└── Observable.java          — главный класс

src/main/java/rx/disposable/
├── Disposable.java          — интерфейс отмены подписки
└── BooleanDisposable.java   — отслеживание состояния подписки

src/main/java/rx/schedulers/
└── Scheduler.java           — интерфейс + три реализации:
                               IOThreadScheduler (CachedThreadPool)
                               ComputationScheduler (FixedThreadPool)
                               SingleThreadScheduler (SingleThread)

src/test/java/rx/
└── ObservableTest.java      — тесты по всем компонентам
```


---

## Как работает паттерн Observable/Observer

Observable - это объект, описывающий поток данных и набор преобразований над ним. Не выполняет никаких действий до вызова метода subscribe().

Операторы не модифицируют Observable - каждый оператор возвращает новый объект Observable, содержащий ссылку на предыдущий источник данных и логику соответствующего преобразования. В результате последовательность операторов формирует цепочку взаимосвязанных объектов Observable. Выполнение этой цепочки начинается только после вызова метода subscribe().

Во время подписки каждый Observable подписывается на предыдущий элемент цепочки, пока не будет достигнут исходный источник данных. После этого источник начинает передавать элементы, которые последовательно проходят через все операторы и поступают конечному подписчику.

В итоге создание цепочки операторов является лишь описанием процесса обработки данных, а фактическое выполнение начинается только при подписке.

---

## Операторы

### map(Function<T, R>)
Преобразует каждый элемент: один вход - один выход.

Observable.just(1, 2, 3).map(i -> i * 2)  // → 2, 4, 6


### filter(Predicate<T>)
Пропускает только элементы, удовлетворяющие условию.

Observable.just(1,2,3,4).filter(i -> i % 2 == 0)  // → 2, 4


### flatMap(Function<T, Observable<R>>)
Каждый элемент преобразуется в поток, потоки объединяются.

Observable.just(1, 2)
    .flatMap(i -> Observable.just(i * 10, i * 100)) // → 10, 100, 20, 200

Порядок при асинхронном исполнении не гарантирован.
Для гарантированного порядка используют concatMap() (в данной реализации не включён).


---

## Schedulers: принципы и различия

IOThreadScheduler использует CachedThreadPool (неограниченное число потоков) для операций с вводом‑выводом (сеть, файлы, БД).
ComputationScheduler использует FixedThreadPool (число потоков = числу ядер CPU) для CPU‑интенсивных задач (парсинг, шифрование).
SingleThreadScheduler работает в одном потоке для последовательного выполнения (обновление UI, логи).


---

### Сравнение subscribeOn и observeOn

Observable.create(...)   - работает в subscribeOn-потоке
    .subscribeOn(io)
    .map(...)             - всё ещё в subscribeOn-потоке
    .observeOn(single)
    .subscribe(observer)  - observer получает данные в observeOn-потоке


subscribeOn влияет на поток источника данных.
observeOn влияет на поток наблюдателя (где обрабатываются данные).
Только первый subscribeOn в цепочке имеет эффект.
Каждый observeOn переключает поток для всего, что идёт после него.


---

## Disposable и управление ресурсами

Внутри Observable источник должен проверять emitter.isDisposed() в цикле, чтобы прекратить работу. Без этой проверки источник продолжает работать даже после dispose().

ObservableEmitter автоматически вызывает dispose() после onError/onComplete - чтобы не было "висящих" подписок.


---

## Тестирование

### Синхронные тесты
Используют простой список для сбора результатов:

List<Integer> results = new ArrayList<>();
Observable.just(1,2,3).map(i -> i*2).subscribe(results::add);
assertEquals(List.of(2,4,6), results);


### Асинхронные тесты
Используют `CountDownLatch` для ожидания завершения:

CountDownLatch latch = new CountDownLatch(1);
Observable.just(42)
    .subscribeOn(new IOThreadScheduler())
    .subscribe(item -> {}, e -> {}, latch::countDown);
assertTrue(latch.await(3, TimeUnit.SECONDS));


### Покрытые сценарии
- Базовые компоненты: just, create, empty, error.
- Операторы: map (включая смену типа), filter, flatMap.
- Цепочки операторов.
- Ошибки в операторах → onError.
- subscribeOn и observeOn (проверка имён потоков).
- Disposable: отмена подписки, идемпотентность.
- Контракт потока: после onError/onComplete - тишина.

---

## Примеры использования

### Базовый пример

Observable.just(1, 2, 3, 4, 5)
    .filter(i -> i % 2 == 0)
    .map(i -> "Чётное: " + i)
    .subscribe(System.out::println);
// Чётное: 2
// Чётное: 4


### Асинхронная загрузка данных

Scheduler io = new Scheduler.IOThreadScheduler();
Scheduler ui = new Scheduler.SingleThreadScheduler();

Observable.<String>create(emitter -> {
    String data = fetchFromNetwork(); // долгая операция
    emitter.onNext(data);
    emitter.onComplete();
})
.subscribeOn(io)    // fetchFromNetwork() - в IO-потоке
.observeOn(ui)      // обновление экрана - в UI-потоке
.subscribe(
    data -> updateScreen(data),
    error -> showError(error)
);


### Преобразование с flatMap

Observable.just("user1", "user2")
    .flatMap(userId -> loadUserOrders(userId)) // каждый userId → поток заказов
    .filter(order -> order.getAmount() > 100)
    .subscribe(order -> processOrder(order));


### Отмена подписки

Disposable d = Observable.just(1, 2, 3, 4, 5)
    .subscribe(item -> {
        System.out.println(item);
        if (item == 3) d.dispose(); // остановиться на 3
    });

