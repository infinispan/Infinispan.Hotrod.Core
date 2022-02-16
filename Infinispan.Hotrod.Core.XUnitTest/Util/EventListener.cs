using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace Infinispan.Hotrod.Core.Tests.Util
{
    class LoggingEventListener : IClientListener
    {
        public BlockingCollection<Event> createdEvents = new BlockingCollection<Event>();
        public BlockingCollection<Event> removedEvents = new BlockingCollection<Event>();
        public BlockingCollection<Event> modifiedEvents = new BlockingCollection<Event>();
        public BlockingCollection<Event> expiredEvents = new BlockingCollection<Event>();
        public BlockingCollection<Event> customEvents = new BlockingCollection<Event>();

        public void CreatedEventAction(Event e)
        {
            createdEvents.Add(e);
        }

        public void RemovedEventAction(Event e)
        {
            removedEvents.Add(e);
        }

        public void ModifiedEventAction(Event e)
        {
            modifiedEvents.Add(e);
        }

        public void ExpiredEventAction(Event e)
        {
            expiredEvents.Add(e);
        }

        public void CustomEventAction(Event e)
        {
            customEvents.Add(e);
        }
        public Event PollEvent(EventType eventType, byte isCustom = 0)
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;
            tokenSource.CancelAfter(20000);
            try
            {
                if (isCustom != 0)
                {
                    return customEvents.Take(token);
                }
                switch (eventType)
                {
                    case EventType.CREATED:
                        var a = createdEvents.Take(token);
                        return a;
                    case EventType.MODIFIED:
                        return modifiedEvents.Take(token);
                    case EventType.REMOVED:
                        return removedEvents.Take(token);
                    case EventType.EXPIRED:
                        return expiredEvents.Take(token);
                    default:
                        throw new ArgumentException("Uknown event type");
                }
            }
            catch (OperationCanceledException ex)
            {
                throw new TimeoutException("The event of type " + eventType.ToString() + " was not received within timeout!", ex);
            }
        }
        public void OnEvent(Event e)
        {
            if (e.CustomMarker != 0)
            {
                CustomEventAction(e);
                return;
            }
            switch (e.Type)
            {
                case EventType.CREATED:
                    CreatedEventAction(e);
                    break;
                case EventType.MODIFIED:
                    ModifiedEventAction(e);
                    break;
                case EventType.REMOVED:
                    RemovedEventAction(e);
                    break;
                case EventType.EXPIRED:
                    ExpiredEventAction(e);
                    break;
            }
        }
    }
}
