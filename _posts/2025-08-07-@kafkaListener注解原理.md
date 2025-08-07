# @KafkaListener注解

`@KafkaListener`是 Spring Kafka 提供的核心注解，用于声明方法作为 Kafka 消息的监听器，简化消息消费逻辑的实现

```java
// demo
@Service
public class KafkaConsumerService {
    @KafkaListener(
        topics = "my-topic",
        groupId = "my-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void processMessage(ConsumerRecord<String, String> record) {
        System.out.printf("Received message: %s (Partition: %d)%n", 
            record.value(), record.partition());
    }[1,5](@ref)。
}
```

## 源码解析

### 启动点，从`@EnableKafka`开始

```java
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(KafkaListenerConfigurationSelector.class)
public @interface EnableKafka {
}
```

引入`KafkaListenerConfigurationSelector`

```java
@Order
public class KafkaListenerConfigurationSelector implements DeferredImportSelector {

	@Override
	public String[] selectImports(AnnotationMetadata importingClassMetadata) {
		return new String[] { KafkaBootstrapConfiguration.class.getName() };
	}

}
```

引入`KafkaBootstrapConfiguration`

```java
public class KafkaBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(
				KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {

			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerEndpointRegistry.class));
		}
	}
}
```

引入`KafkaListenerAnnotationBeanPostProcessor`，实现了`ImportBeanDefinitionRegistrar`接口，Spring容器启动时自动注册Bean实例

### 注册注解处理器`KafkaListenerAnnotationBeanPostProcessor`实例

#### 核心方法`postProcessAfterInitialization()`处理注解

```java
@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
    // 只从有注解的类中找
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Collection<KafkaListener> classLevelListeners = findListenerAnnotations(targetClass);
			final boolean hasClassLevelListeners = classLevelListeners.size() > 0;
			final List<Method> multiMethods = new ArrayList<>();
      // 通过反射的方式在方法上找注解
			Map<Method, Set<KafkaListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<Set<KafkaListener>>) method -> {
						Set<KafkaListener> listenerMethods = findListenerAnnotations(method);
						return (!listenerMethods.isEmpty() ? listenerMethods : null);
					});
			if (hasClassLevelListeners) {
        // @KafkaListener注解加在类上时，会为该类创建一个消费者实例，类内的方法需要使用@KafkaHandler来处理不同类型的消息比如String、Integer
				Set<Method> methodsWithHandler = MethodIntrospector.selectMethods(targetClass,
						(ReflectionUtils.MethodFilter) method ->
								AnnotationUtils.findAnnotation(method, KafkaHandler.class) != null);
				multiMethods.addAll(methodsWithHandler);
			}
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				this.logger.trace(() -> "No @KafkaListener annotations found on bean type: " + bean.getClass());
			}
			else {
        // 方法级别注解进一步处理
				for (Map.Entry<Method, Set<KafkaListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (KafkaListener listener : entry.getValue()) {
            // 核心逻辑，根据注解上的配置、方法参数等构建消费者实例
						processKafkaListener(listener, method, bean, beanName);
					}
				}
				this.logger.debug(() -> annotatedMethods.size() + " @KafkaListener methods processed on bean '"
							+ beanName + "': " + annotatedMethods);
			}
			if (hasClassLevelListeners) {
        // 类级别注解进一步处理
				processMultiMethodListeners(classLevelListeners, multiMethods, bean, beanName);
			}
		}
		return bean;
	}
```

### 创建消费者的核心代码

```java
protected void processKafkaListener(KafkaListener kafkaListener, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
  	
  	// 动态配置和管理Kafka监听器的核心类，持有消费者实例，并可以通过setMethod绑定消息处理方法
		MethodKafkaListenerEndpoint<K, V> endpoint = new MethodKafkaListenerEndpoint<>();
		endpoint.setMethod(methodToUse);

		String beanRef = kafkaListener.beanRef();
		this.listenerScope.addListener(beanRef, bean);
  	// 订阅指定topic
		String[] topics = resolveTopics(kafkaListener);
  	// 指定拉取的分区和偏移量（如有）
		TopicPartitionOffset[] tps = resolveTopicPartitions(kafkaListener);
  	// 注册主监听器和重试监听器
		if (!processMainAndRetryListeners(kafkaListener, bean, beanName, methodToUse, endpoint, topics, tps)) {
			// 没有配置重试逻辑，直接注册主监听器
      processListener(endpoint, kafkaListener, bean, beanName, topics, tps);
		}
		this.listenerScope.removeListener(beanRef);
	}
```

注册主监听器

```java
protected void processListener(MethodKafkaListenerEndpoint<?, ?> endpoint, KafkaListener kafkaListener,
								Object bean, String beanName, String[] topics, TopicPartitionOffset[] tps) {

		processKafkaListenerAnnotationBeforeRegistration(endpoint, kafkaListener, bean, topics, tps);
		String containerFactory = resolve(kafkaListener.containerFactory());
  	// 获取注解指定的containerFactory
		KafkaListenerContainerFactory<?> listenerContainerFactory = resolveContainerFactory(kafkaListener, containerFactory, beanName);
		// 实际注册
		this.registrar.registerEndpoint(endpoint, listenerContainerFactory);

		processKafkaListenerEndpointAfterRegistration(endpoint, kafkaListener);
	}
```

```java
public void registerEndpoint(KafkaListenerEndpoint endpoint, @Nullable KafkaListenerContainerFactory<?> factory) {
		Assert.notNull(endpoint, "Endpoint must be set");
		Assert.hasText(endpoint.getId(), "Endpoint id must be set");
		// Factory may be null, we defer the resolution right before actually creating the container
		KafkaListenerEndpointDescriptor descriptor = new KafkaListenerEndpointDescriptor(endpoint, factory);
		synchronized (this.endpointDescriptors) {
			if (this.startImmediately) { // Register and start immediately
				this.endpointRegistry.registerListenerContainer(descriptor.endpoint,
						resolveContainerFactory(descriptor), true);
			}
			else {
				this.endpointDescriptors.add(descriptor);
			}
		}
	}
```

底层调用`listenerContainer.start()`启动消费者实例

```java
private void startIfNecessary(MessageListenerContainer listenerContainer) {
  if (this.contextRefreshed || listenerContainer.isAutoStartup()) {
    listenerContainer.start();
  }
}
```

#### 创建消费者

```java
@Override
public final void start() {
  checkGroupId();
  synchronized (this.lifecycleMonitor) {
    if (!isRunning()) {
      Assert.state(this.containerProperties.getMessageListener() instanceof GenericMessageListener,
          () -> "A " + GenericMessageListener.class.getName() + " implementation must be provided");
      // 核心入口
      doStart();
    }
  }
}
```

`ConcurrentMessageListenerContainer.doStart()`该方法会为每个并发线程（由 `concurrency`参数控制）创建 `KafkaMessageListenerContainer`实例，并调用其 `doStart()`方法

```java
@Override
protected void doStart() {
  if (!isRunning()) {
    checkTopics();
    ContainerProperties containerProperties = getContainerProperties();
    TopicPartitionOffset[] topicPartitions = containerProperties.getTopicPartitions();
    if (topicPartitions != null && this.concurrency > topicPartitions.length) {
      this.logger.warn(() -> "When specific partitions are provided, the concurrency must be less than or "
          + "equal to the number of partitions; reduced from " + this.concurrency + " to "
          + topicPartitions.length);
      this.concurrency = topicPartitions.length;
    }
    setRunning(true);

    for (int i = 0; i < this.concurrency; i++) {
      // 完成配置
      KafkaMessageListenerContainer<K, V> container =
          constructContainer(containerProperties, topicPartitions, i);
      configureChildContainer(i, container);
      if (isPaused()) {
        container.pause();
      }
      // 启动
      container.start();
      this.containers.add(container);
    }
  }
}
```

```java
@Override
protected void doStart() {
  if (isRunning()) {
    return;
  }
  if (this.clientIdSuffix == null) { // stand-alone container
    checkTopics();
  }
  ContainerProperties containerProperties = getContainerProperties();
  checkAckMode(containerProperties);

  Object messageListener = containerProperties.getMessageListener();
  AsyncListenableTaskExecutor consumerExecutor = containerProperties.getConsumerTaskExecutor();
  if (consumerExecutor == null) {
    consumerExecutor = new SimpleAsyncTaskExecutor(
        (getBeanName() == null ? "" : getBeanName()) + "-C-");
    containerProperties.setConsumerTaskExecutor(consumerExecutor);
  }
  GenericMessageListener<?> listener = (GenericMessageListener<?>) messageListener;
  ListenerType listenerType = determineListenerType(listener);
  // 构建消费者实例：通过原生的new KafkaConsumer构建实例
  this.listenerConsumer = new ListenerConsumer(listener, listenerType);
  setRunning(true);
  this.startLatch = new CountDownLatch(1);
  this.listenerConsumerFuture = consumerExecutor
      .submitListenable(this.listenerConsumer);
  try {
    if (!this.startLatch.await(containerProperties.getConsumerStartTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
      this.logger.error("Consumer thread failed to start - does the configured task executor "
          + "have enough threads to support all containers and concurrency?");
      publishConsumerFailedToStart();
    }
  }
  catch (@SuppressWarnings(UNUSED) InterruptedException e) {
    Thread.currentThread().interrupt();
  }
}
```

#### 最终通过 `KafkaConsumerThread`启动原生 Kafka 消费者线程

```java
// org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#run
@Override // NOSONAR complexity
public void run() {
  ListenerUtils.setLogOnlyMetadata(this.containerProperties.isOnlyLogRecordMetadata());
  publishConsumerStartingEvent();
  this.consumerThread = Thread.currentThread();
  setupSeeks();
  KafkaUtils.setConsumerGroupId(this.consumerGroupId);
  this.count = 0;
  this.last = System.currentTimeMillis();
  initAssignedPartitions();
  publishConsumerStartedEvent();
  Throwable exitThrowable = null;
  while (isRunning()) {
    try {
      pollAndInvoke();
    }
    ...
  }
  wrapUp(exitThrowable);
}
```

拉取消息

```java
protected void pollAndInvoke() {
  if (!this.autoCommit && !this.isRecordAck) {
    processCommits();
  }
  fixTxOffsetsIfNeeded();
  idleBetweenPollIfNecessary();
  if (this.seeks.size() > 0) {
    processSeeks();
  }
  pauseConsumerIfNecessary();
  pausePartitionsIfNecessary();
  this.lastPoll = System.currentTimeMillis();
  if (!isRunning()) {
    return;
  }
  this.polling.set(true);
  // 调用consumer.poll()kafka原生api
  ConsumerRecords<K, V> records = doPoll();
  if (!this.polling.compareAndSet(true, false) && records != null) {
    /*
     * There is a small race condition where wakeIfNecessary was called between
     * exiting the poll and before we reset the boolean.
     */
    if (records.count() > 0) {
      this.logger.debug(() -> "Discarding polled records, container stopped: " + records.count());
    }
    return;
  }
  resumeConsumerIfNeccessary();
  if (!this.consumerPaused) {
    resumePartitionsIfNecessary();
  }
  debugRecords(records);

  invokeIfHaveRecords(records);
}
```

