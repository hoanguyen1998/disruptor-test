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
            Thread.Sleep(1);
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

        private int producerCount = 1;
        private int runCount = 10000;
        private int RingBufferAndCapacitySize = 1024;

        [TestCase()]
        public async Task TestBoth()
        {
            for (int i = 0; i < 1; i++)
            {
                foreach (var rs in new int[] {1024, 2048 /*,4096,4096*2*/})
                {
                    Console.WriteLine($"RingBufferAndCapacitySize:{rs}, producerCount:{producerCount}, runCount:{runCount} of {i}");
                    RingBufferAndCapacitySize = rs;
                    await DisruptorTest();
                    await BlockingCollectionTest();
                    await MultiProducerDisruptorTest();
                }
            }
        }

        [TestCase()]
        public async Task NormalCollectionTest()
        {
            var sw = new Stopwatch();
            // BlockingCollection<ValueEntry> dataItems = new BlockingCollection<ValueEntry>(RingBufferAndCapacitySize);

            sw.Start();

            // collectionAddEnded = false;

            // A simple blocking consumer with no cancellation.
            // var task = Task.Factory.StartNew(() =>
            // {
            //     while (!collectionAddEnded && !dataItems.IsCompleted)
            //     {
            //         //if (!dataItems.IsCompleted && dataItems.TryTake(out var ve))
            //         if (dataItems.TryTake(out var ve))
            //         {
            //         }
            //     }
            // }, TaskCreationOptions.LongRunning);


            // var tasks = new Task[producerCount];
            // for (int t = 0; t < producerCount; t++)
            // {
            //     tasks[t] = Task.Run(() =>
            //     {
                    for (int i = 0; i < runCount; i++)
                    {
                        ValueEntry entry = new ValueEntry();
                        entry.Id = i;

                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // dataItems.Add(entry);
                        Thread.Sleep(1);
                    }
                // });
            // }

            // await Task.WhenAll(tasks);

            // collectionAddEnded = true;
            // await task;

            sw.Stop();

            Console.WriteLine($"NormalCollectionTest Time:{sw.ElapsedMilliseconds/1000d}");
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
                    // if (!dataItems.IsCompleted && dataItems.TryTake(out var ve))
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

                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // dataItems.Add(entry);
                        Thread.Sleep(1);
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


            // var tasks = new Task[producerCount];
            // for (int t = 0; t < producerCount; t++)
            // {
            //     tasks[t] = Task.Run(() =>
            //     {
                    for (int i = 0; i < runCount; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer.Next();
                        _ringBuffer[sequenceNo].Id = 0;
                        _ringBuffer.Publish(sequenceNo);
                    }
                // });
            // }


            // await Task.WhenAll(tasks);


            disruptor.Shutdown();

            sw.Stop();
            Console.WriteLine($"DisruptorTest Time:{sw.ElapsedMilliseconds/1000d}s");
        }

        [TestCase()]
        public async Task MultiProducerDisruptorTest()
        {
            var disruptor1 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());
            var disruptor2 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());

            var disruptor3 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());

            var disruptor5 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());

            var disruptor6 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Single, new BlockingWaitStrategy());        

            var disruptor4 =
                new Disruptor.Dsl.Disruptor<ValueEntry>(() => new ValueEntry(), RingBufferAndCapacitySize, TaskScheduler.Default,
                    ProducerType.Multi, new BlockingWaitStrategy());
            

            var _ringBuffer4 = disruptor4.RingBuffer;

            disruptor1.HandleEventsWith(new ProducerHandler(_ringBuffer4));
            disruptor2.HandleEventsWith(new ProducerHandler(_ringBuffer4));
            disruptor3.HandleEventsWith(new ProducerHandler(_ringBuffer4));
            disruptor5.HandleEventsWith(new ProducerHandler(_ringBuffer4));
            disruptor6.HandleEventsWith(new ProducerHandler(_ringBuffer4));
            disruptor4.HandleEventsWith(new MyHandler());
            
            var _ringBuffer1 = disruptor1.Start();
            var _ringBuffer2 = disruptor2.Start();
            var _ringBuffer3 = disruptor3.Start();
            var _ringBuffer5 = disruptor5.Start();
            var _ringBuffer6 = disruptor6.Start();
            disruptor4.Start();

            Stopwatch sw = Stopwatch.StartNew();

            sw.Start();


            var tasks = new Task[5];
            // for (int t = 0; t < producerCount; t++)
            // {
                tasks[0] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount / 5; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer1.Next();
                        _ringBuffer1[sequenceNo].Id = 0;
                        _ringBuffer1.Publish(sequenceNo);
                    }
                });

                tasks[1] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount / 5; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer2.Next();
                        _ringBuffer2[sequenceNo].Id = 0;
                        _ringBuffer2.Publish(sequenceNo);
                    }
                });

                tasks[2] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount / 5; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer3.Next();
                        _ringBuffer3[sequenceNo].Id = 0;
                        _ringBuffer3.Publish(sequenceNo);
                    }
                });

                tasks[3] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount / 5; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer5.Next();
                        _ringBuffer5[sequenceNo].Id = 0;
                        _ringBuffer5.Publish(sequenceNo);
                    }
                });
                tasks[4] = Task.Run(() =>
                {
                    for (int i = 0; i < runCount / 5; i++)
                    {
                        string str1 = "8=FIX.4.2\x01" + "9=45\x01" + "35=0\x01" + "34=3\x01" + "49=TW\x01" +
                "52=20000426-12:05:06\x01" + "56=ISLD";

                        var msg = str1.Split("\x01");

                        var dict = new Dictionary<string, string>();

                        foreach (var item in msg)
                        {
                            var val = item.Split("=");
                            dict[val[0]] = val[1];
                        }
                        // Thread.Sleep(1);
                        long sequenceNo = _ringBuffer6.Next();
                        _ringBuffer6[sequenceNo].Id = 0;
                        _ringBuffer6.Publish(sequenceNo);
                    }
                });
            // }


            await Task.WhenAll(tasks);


            disruptor1.Shutdown();
            disruptor2.Shutdown();
            disruptor3.Shutdown();
            disruptor4.Shutdown();

            sw.Stop();
            Console.WriteLine($"Multiproducer DisruptorTest Time:{sw.ElapsedMilliseconds/1000d}s");
        }
    }
}