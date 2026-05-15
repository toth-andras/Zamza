using Microsoft.Extensions.Logging;

namespace Zamza.Server.UnitTests.Utils;

internal sealed class LoggerFake<T> : ILogger<T>
{
    private readonly List<string> _logs = [];

    public IReadOnlyList<string> LoggedMessages => _logs.AsReadOnly();
    
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        var message = formatter(state, exception);
        _logs.Add(message);
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
}