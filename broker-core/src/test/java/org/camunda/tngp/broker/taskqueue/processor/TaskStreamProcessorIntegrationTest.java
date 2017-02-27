package org.camunda.tngp.broker.taskqueue.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.protocol.clientapi.EventType.TASK_EVENT;
import static org.camunda.tngp.test.util.BufferAssert.assertThatBuffer;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.camunda.tngp.broker.logstreams.BrokerEventMetadata;
import org.camunda.tngp.broker.taskqueue.data.TaskEvent;
import org.camunda.tngp.broker.taskqueue.data.TaskEventType;
import org.camunda.tngp.broker.transport.clientapi.CommandResponseWriter;
import org.camunda.tngp.broker.transport.clientapi.SubscribedEventWriter;
import org.camunda.tngp.hashindex.store.FileChannelIndexStore;
import org.camunda.tngp.logstreams.LogStreams;
import org.camunda.tngp.logstreams.log.BufferedLogStreamReader;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.log.LogStreamWriter;
import org.camunda.tngp.logstreams.log.LoggedEvent;
import org.camunda.tngp.logstreams.processor.StreamProcessor;
import org.camunda.tngp.logstreams.processor.StreamProcessorController;
import org.camunda.tngp.logstreams.spi.SnapshotStorage;
import org.camunda.tngp.protocol.clientapi.SubscriptionType;
import org.camunda.tngp.test.util.FluentMock;
import org.camunda.tngp.test.util.agent.ControllableAgentRunnerService;
import org.camunda.tngp.util.time.ClockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.MockitoAnnotations;

public class TaskStreamProcessorIntegrationTest
{
    private static final int LOG_ID = 1;

    private static final byte[] TASK_TYPE = "test-task".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAYLOAD = "payload".getBytes();

    private static final DirectBuffer TASK_TYPE_BUFFER = new UnsafeBuffer(TASK_TYPE);

    private LogStream logStream;

    private LockTaskStreamProcessor lockTaskStreamProcessor;
    private TaskExpireLockStreamProcessor taskExpireLockStreamProcessor;

    private StreamProcessorController taskInstanceStreamProcessorController;
    private StreamProcessorController taskSubscriptionStreamProcessorController;
    private StreamProcessorController taskExpireLockStreamProcessorController;

    @Rule
    public ControllableAgentRunnerService agentRunnerService = new ControllableAgentRunnerService();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public Timeout testTimeout = Timeout.seconds(5);

    @FluentMock
    private CommandResponseWriter mockResponseWriter;

    @FluentMock
    private SubscribedEventWriter mockSubscribedEventWriter;

    private LogStreamWriter logStreamWriter;

    private final BrokerEventMetadata defaultBrokerEventMetadata = new BrokerEventMetadata();
    private final BrokerEventMetadata lastBrokerEventMetadata = new BrokerEventMetadata();

    private final TaskEvent followUpTaskEvent = new TaskEvent();

    @Before
    public void setup() throws InterruptedException, ExecutionException
    {
        MockitoAnnotations.initMocks(this);

        when(mockResponseWriter.tryWriteResponse()).thenReturn(true);
        when(mockSubscribedEventWriter.tryWriteMessage()).thenReturn(true);

        doAnswer(invocation ->
        {
            final BrokerEventMetadata metadata =  (BrokerEventMetadata) invocation.getArguments()[0];

            final UnsafeBuffer metadataBuffer = new UnsafeBuffer(new byte[metadata.getLength()]);
            metadata.write(metadataBuffer, 0);

            lastBrokerEventMetadata.wrap(metadataBuffer, 0, metadataBuffer.capacity());

            return invocation.getMock();
        }).when(mockResponseWriter).brokerEventMetadata(any(BrokerEventMetadata.class));

        final String rootPath = tempFolder.getRoot().getAbsolutePath();

        logStream = LogStreams.createFsLogStream("test-log", LOG_ID)
            .logRootPath(rootPath)
            .agentRunnerService(agentRunnerService)
            .writeBufferAgentRunnerService(agentRunnerService)
            .deleteOnClose(true)
            .build();

        logStream.openAsync();

        final SnapshotStorage snapshotStorage = LogStreams.createFsSnapshotStore(rootPath).build();
        final FileChannelIndexStore indexStore = FileChannelIndexStore.tempFileIndexStore();

        final StreamProcessor taskInstanceStreamProcessor = new TaskInstanceStreamProcessor(mockResponseWriter, mockSubscribedEventWriter, indexStore);
        taskInstanceStreamProcessorController = LogStreams.createStreamProcessor("task-instance", 0, taskInstanceStreamProcessor)
            .sourceStream(logStream)
            .targetStream(logStream)
            .snapshotStorage(snapshotStorage)
            .agentRunnerService(agentRunnerService)
            .build();

        lockTaskStreamProcessor = new LockTaskStreamProcessor(TASK_TYPE_BUFFER);
        taskSubscriptionStreamProcessorController = LogStreams.createStreamProcessor("task-lock", 1, lockTaskStreamProcessor)
            .sourceStream(logStream)
            .targetStream(logStream)
            .snapshotStorage(snapshotStorage)
            .agentRunnerService(agentRunnerService)
            .build();

        taskExpireLockStreamProcessor = new TaskExpireLockStreamProcessor();
        taskExpireLockStreamProcessorController = LogStreams.createStreamProcessor("task-expire-lock", 2, taskExpireLockStreamProcessor)
                .sourceStream(logStream)
                .targetStream(logStream)
                .snapshotStorage(snapshotStorage)
                .agentRunnerService(agentRunnerService)
                .build();

        taskInstanceStreamProcessorController.openAsync();
        taskSubscriptionStreamProcessorController.openAsync();
        taskExpireLockStreamProcessorController.openAsync();

        logStreamWriter = new LogStreamWriter(logStream);
        defaultBrokerEventMetadata.eventType(TASK_EVENT);

        agentRunnerService.waitUntilDone();
    }

    @After
    public void cleanUp() throws Exception
    {
        taskInstanceStreamProcessorController.closeAsync();
        taskSubscriptionStreamProcessorController.closeAsync();
        taskExpireLockStreamProcessorController.closeAsync();

        logStream.closeAsync();

        ClockUtil.reset();
    }

    @Test
    public void shouldCreateTask() throws InterruptedException, ExecutionException
    {
        // given
        final TaskEvent taskEvent = new TaskEvent()
            .setEventType(TaskEventType.CREATE)
            .setType(TASK_TYPE_BUFFER, 0, TASK_TYPE_BUFFER.capacity())
            .setPayload(new UnsafeBuffer(PAYLOAD));

        // when
        final long position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        // then
        assertThatEventIsFollowedBy(position, TaskEventType.CREATED);

        assertThatBuffer(followUpTaskEvent.getType()).hasBytes(TASK_TYPE_BUFFER.byteArray());

        assertThatBuffer(followUpTaskEvent.getPayload())
            .hasCapacity(PAYLOAD.length)
            .hasBytes(PAYLOAD);

        verify(mockResponseWriter).topicId(LOG_ID);
        verify(mockResponseWriter).longKey(2L);
        verify(mockResponseWriter).tryWriteResponse();
    }

    @Test
    public void shouldLockTask() throws InterruptedException, ExecutionException
    {
        // given
        final TaskSubscription subscription = new TaskSubscription()
                .setId(1L)
                .setChannelId(11)
                .setTaskType(TASK_TYPE_BUFFER)
                .setLockDuration(Duration.ofMinutes(5).toMillis())
                .setLockOwner(1)
                .setCredits(10);

        lockTaskStreamProcessor.addSubscription(subscription);

        final TaskEvent taskEvent = new TaskEvent()
            .setEventType(TaskEventType.CREATE)
            .setType(TASK_TYPE_BUFFER, 0, TASK_TYPE_BUFFER.capacity())
            .setPayload(new UnsafeBuffer(PAYLOAD));

        // when
        final long position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        // then
        LoggedEvent event = assertThatEventIsFollowedBy(position, TaskEventType.CREATED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        assertThat(followUpTaskEvent.getLockTime()).isGreaterThan(0);
        assertThat(followUpTaskEvent.getLockOwner()).isEqualTo(1);

        assertThatBuffer(followUpTaskEvent.getPayload())
            .hasCapacity(PAYLOAD.length)
            .hasBytes(PAYLOAD);

        verify(mockResponseWriter, times(1)).tryWriteResponse();

        verify(mockSubscribedEventWriter, times(1)).tryWriteMessage();
        verify(mockSubscribedEventWriter).channelId(11);
        verify(mockSubscribedEventWriter).subscriptionId(1L);
        verify(mockSubscribedEventWriter).subscriptionType(SubscriptionType.TASK_SUBSCRIPTION);
    }

    @Test
    public void shouldCompleteTask() throws InterruptedException, ExecutionException
    {
        // given
        final TaskSubscription subscription = new TaskSubscription()
                .setId(1L)
                .setChannelId(11)
                .setTaskType(TASK_TYPE_BUFFER)
                .setLockDuration(Duration.ofMinutes(5).toMillis())
                .setLockOwner(3)
                .setCredits(10);

        lockTaskStreamProcessor.addSubscription(subscription);

        final TaskEvent taskEvent = new TaskEvent()
            .setEventType(TaskEventType.CREATE)
            .setType(TASK_TYPE_BUFFER, 0, TASK_TYPE_BUFFER.capacity())
            .setPayload(new UnsafeBuffer(PAYLOAD));

        long position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        LoggedEvent event = assertThatEventIsFollowedBy(position, TaskEventType.CREATED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        // when
        final byte[] modifiedPayload = "modified payload".getBytes();

        taskEvent
            .setEventType(TaskEventType.COMPLETE)
            .setLockOwner(3)
            .setPayload(new UnsafeBuffer(modifiedPayload));

        position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        // then
        event = assertThatEventIsFollowedBy(position, TaskEventType.COMPLETED);

        assertThatBuffer(followUpTaskEvent.getPayload())
            .hasCapacity(modifiedPayload.length)
            .hasBytes(modifiedPayload);

        verify(mockResponseWriter, times(2)).tryWriteResponse();
    }

    @Test
    public void shouldFailTask() throws InterruptedException, ExecutionException
    {
        // given
        final TaskSubscription subscription = new TaskSubscription()
                .setId(1L)
                .setChannelId(11)
                .setTaskType(TASK_TYPE_BUFFER)
                .setLockDuration(Duration.ofMinutes(5).toMillis())
                .setLockOwner(3)
                .setCredits(10);

        lockTaskStreamProcessor.addSubscription(subscription);

        final TaskEvent taskEvent = new TaskEvent()
            .setEventType(TaskEventType.CREATE)
            .setType(TASK_TYPE_BUFFER, 0, TASK_TYPE_BUFFER.capacity())
            .setPayload(new UnsafeBuffer(PAYLOAD));

        long position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        LoggedEvent event = assertThatEventIsFollowedBy(position, TaskEventType.CREATED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        // when
        taskEvent
            .setEventType(TaskEventType.FAIL)
            .setLockOwner(3);

        position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        // then
        event = assertThatEventIsFollowedBy(position, TaskEventType.FAILED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        verify(mockResponseWriter, times(2)).tryWriteResponse();
    }

    @Test
    public void shouldExpireTaskLock() throws InterruptedException, ExecutionException
    {
        // given
        final Instant now = Instant.now();
        final Duration lockDuration = Duration.ofMinutes(5);

        ClockUtil.setCurrentTime(now);

        final TaskSubscription subscription = new TaskSubscription()
                .setId(1L)
                .setChannelId(11)
                .setTaskType(TASK_TYPE_BUFFER)
                .setLockDuration(lockDuration.toMillis())
                .setLockOwner(3)
                .setCredits(10);

        lockTaskStreamProcessor.addSubscription(subscription);

        final TaskEvent taskEvent = new TaskEvent()
            .setEventType(TaskEventType.CREATE)
            .setType(TASK_TYPE_BUFFER, 0, TASK_TYPE_BUFFER.capacity())
            .setPayload(new UnsafeBuffer(PAYLOAD));

        final long position = logStreamWriter
            .key(2L)
            .metadataWriter(defaultBrokerEventMetadata)
            .valueWriter(taskEvent)
            .tryWrite();

        LoggedEvent event = assertThatEventIsFollowedBy(position, TaskEventType.CREATED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        // when
        ClockUtil.setCurrentTime(now.plus(lockDuration));

        taskExpireLockStreamProcessor.checkLockExpirationAsync();

        // then
        event = assertThatEventIsFollowedBy(event, TaskEventType.EXPIRE_LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK_EXPIRED);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCK);
        event = assertThatEventIsFollowedBy(event, TaskEventType.LOCKED);

        assertThat(followUpTaskEvent.getLockTime()).isGreaterThan(now.plus(lockDuration).toEpochMilli());
        assertThat(followUpTaskEvent.getLockOwner()).isEqualTo(3);

        assertThatBuffer(followUpTaskEvent.getPayload())
            .hasCapacity(PAYLOAD.length)
            .hasBytes(PAYLOAD);

        verify(mockResponseWriter, times(1)).tryWriteResponse();
        verify(mockSubscribedEventWriter, times(2)).tryWriteMessage();
    }

    private LoggedEvent assertThatEventIsFollowedBy(LoggedEvent event, TaskEventType eventType)
    {
        return assertThatEventIsFollowedBy(event.getPosition(), eventType);
    }

    private LoggedEvent assertThatEventIsFollowedBy(long position, TaskEventType eventType)
    {
        final LoggedEvent event = getResultEventOf(position);

        followUpTaskEvent.reset();
        event.readValue(followUpTaskEvent);

        assertThat(followUpTaskEvent.getEventType()).isEqualTo(eventType);

        return event;
    }

    private LoggedEvent getResultEventOf(long position)
    {
        agentRunnerService.waitUntilDone();

        final BufferedLogStreamReader logStreamReader = new BufferedLogStreamReader(logStream);

        LoggedEvent loggedEvent = null;
        long sourceEventPosition = -1;

        while (sourceEventPosition < position)
        {
            if (logStreamReader.hasNext())
            {
                loggedEvent = logStreamReader.next();
                sourceEventPosition = loggedEvent.getSourceEventPosition();
            }
        }

        assertThat(sourceEventPosition).isEqualTo(position);

        return loggedEvent;
    }
}