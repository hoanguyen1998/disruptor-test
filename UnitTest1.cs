using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Disruptor;
using Disruptor.Dsl;
using NUnit.Framework;

namespace DisruptorTest.Ds
{
    public sealed class ValueEntry
    {
        internal int Id { get; set; }
    }

    class MyHandler : IEventHandler<ValueEntry>
    {
        public void OnEvent(ValueEntry data, long sequence, bool endOfBatch)
        {
        }
    }

    class ProducerHandler : IEventHandler<ValueEntry>
    {
        private RingBuffer<ValueEntry> _ringBuffer;
        public ProducerHandler(RingBuffer<ValueEntry> ringBuffer)
        {
            _ringBuffer = ringBuffer;
        }
        public void OnEvent(ValueEntry data, long sequence, bool endOfBatch)
        {
            long sequenceNo = _ringBuffer.Next();
            _ringBuffer[sequenceNo].Id = 0;
            _ringBuffer.Publish(sequenceNo);
        }
    }

    [TestFixture]
    public class DisruptorPerformanceTest
    {
        private volatile bool collectionAddEnded;

        private int producerCount = 10;
        private int runCount = 1000000;
        private int RingBufferAndCapacitySize = 1024;

        [TestCase()]
        public async Task TestBoth()
        {
            for (int i = 0; i < 1; i++)
            {
                foreach (var rs in new int[] {64, 512, 1024, 2048 /*,4096,4096*2*/})
                {
                    Console.WriteLine($"RingBufferAndCapacitySize:{rs}, producerCount:{producerCount}, runCount:{runCount} of {i}");
                    RingBufferAndCapacitySize = rs;
                    await DisruptorTest();
                    await BlockingCollectionTest();
                    await MultiproducerDisruptorTest();
                }
            }
        }

        [TestCase()]
        public async Task BlockingCollectionTest()
        {
            var sw = new Stopwatch();
            BlockingCollection<ValueEntry> dataItems = new BlockingCollection<ValueEntry>(RingBufferAndCapacitySize);

            sw.Start();

            collectionAddEnded = false;

            // A simple blocking consumer with no cancellation.
            var task = Task.Factory.StartNew(() =>
            {
                while (!collectionAddEnded && !dataItems.IsCompleted)
                {
                    //if (!dataItems.IsCompleted && dataItems.TryTake(out var ve))
                    if (dataItems.TryTake(out var ve))
                    {
                    }
                }
            }, TaskCreationOptions.LongRunning);


            var tasks = new Task[producerCount];
            for (int t = 0; t < producerCount; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount; i++)
                    {
                        ValueEntry entry = new ValueEntry();
                        entry.Id = i;
                        dataItems.Add(entry);
                    }
                });
            }

            await Task.WhenAll(tasks);

            collectionAddEnded = true;
            await task;

            sw.Stop();

            Console.WriteLine($"BlockingCollectionTest Time:{sw.ElapsedMilliseconds/1000d}");
        }


        [TestCase()]
        public async Task DisruptorTest()
        {
            var disruptor =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    producerCount > 1 ? ProducerType.Multi : ProducerType.Single, new BlockingWaitStrategy());
            disruptor.HandleEventsWith(new MyHandler());

            var _ringBuffer = disruptor.Start();

            Stopwatch sw = Stopwatch.StartNew();

            sw.Start();


            var tasks = new Task[producerCount];
            for (int t = 0; t < producerCount; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount; i++)
                    {
                        long sequenceNo = _ringBuffer.Next();
                        _ringBuffer[sequenceNo].Id = 0;
                        _ringBuffer.Publish(sequenceNo);
                    }
                });
            }


            await Task.WhenAll(tasks);

            disruptor.Shutdown();

            sw.Stop();
            Console.WriteLine($"DisruptorTest Time:{sw.ElapsedMilliseconds/1000d}s");
        }

        [TestCase()]
        public async Task MultiproducerDisruptorTest()
        {
            var disruptor1 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());
            disruptor1.HandleEventsWith(new MyHandler());
             var disruptor2 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Multi, new BlockingWaitStrategy());
            var _ringBuffer2 = disruptor2.RingBuffer;
            disruptor1.HandleEventsWith(new ProducerHandler(_ringBuffer2));
            disruptor1.HandleEventsWith(new ProducerHandler(_ringBuffer2));
            disruptor1.HandleEventsWith(new ProducerHandler(_ringBuffer2));
            disruptor1.HandleEventsWith(new ProducerHandler(_ringBuffer2));
            disruptor2.HandleEventsWith(new MyHandler());

            
            disruptor2.Start();
            var _ringBuffer1 = disruptor1.Start();

            Stopwatch sw = Stopwatch.StartNew();

            sw.Start();


            var tasks = new Task[1];
            for (int t = 0; t < 1; t++)
            {
                tasks[t] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount; i++)
                    {
                        long sequenceNo = _ringBuffer1.Next();
                        _ringBuffer1[sequenceNo].Id = 0;
                        _ringBuffer1.Publish(sequenceNo);
                    }
                });
            }


            await Task.WhenAll(tasks);


            disruptor1.Shutdown();
            disruptor2.Shutdown();

            sw.Stop();
            Console.WriteLine($"MultiproducerDisruptorTest Time:{sw.ElapsedMilliseconds/1000d}s");
        }
    }
}