namespace Infinispan.Hotrod.Core
{
    internal interface ILogHandler
    {
        public void Log(BeetleX.EventArgs.LogType type, string message);
    }
}