namespace Zamza.Consumer.Internal.ConsumerState;

internal sealed class ConsumerState
{
    public ConsumerStateEnum CurrentState { get; private set; }

    public ConsumerState()
    {
        CurrentState = ConsumerStateEnum.ProcessKafka;
    }

    public void ChangeState(ConsumerStateEnum newState)
    {
        switch (CurrentState)
        {
            case ConsumerStateEnum.Stopped:
                ValidateForStopped(newState);
                break;
            
            case ConsumerStateEnum.ZamzaServerNotAvailable:
                ValidateForZamzaServerNotAvailable(newState);
                break;
            
            case ConsumerStateEnum.PartitionOwnershipClaimRequired:
                ValidateForPartitionOwnershipClaimRequired(newState);
                break;
            
            case ConsumerStateEnum.ProcessKafka:
                ValidateForProcessKafka(newState);
                break;
            
            case ConsumerStateEnum.ProcessZamza:
                ValidateForProcessZamza(newState);
                break;
        }
        
        CurrentState = newState;
    }

    private void ValidateForStopped(ConsumerStateEnum state)
    {
        if (state is not ConsumerStateEnum.Stopped)
        {
            Throw();
        }
    }
    private void ValidateForZamzaServerNotAvailable(ConsumerStateEnum state)
    {
        if (state is 
            ConsumerStateEnum.Stopped or 
            ConsumerStateEnum.ZamzaServerNotAvailable or 
            ConsumerStateEnum.PartitionOwnershipClaimRequired)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForPartitionOwnershipClaimRequired(ConsumerStateEnum state)
    {
        if (state is 
            ConsumerStateEnum.Stopped or
            ConsumerStateEnum.ZamzaServerNotAvailable or
            ConsumerStateEnum.PartitionOwnershipClaimRequired or
            ConsumerStateEnum.ProcessKafka or
            ConsumerStateEnum.ProcessZamza)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForProcessKafka(ConsumerStateEnum state)
    {
        if (state is
            ConsumerStateEnum.Stopped or
            ConsumerStateEnum.ZamzaServerNotAvailable or
            ConsumerStateEnum.PartitionOwnershipClaimRequired or
            ConsumerStateEnum.ProcessKafka or
            ConsumerStateEnum.ProcessZamza)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForProcessZamza(ConsumerStateEnum state)
    {
        if (state is
            ConsumerStateEnum.Stopped or
            ConsumerStateEnum.ZamzaServerNotAvailable or
            ConsumerStateEnum.PartitionOwnershipClaimRequired or
            ConsumerStateEnum.ProcessKafka or
            ConsumerStateEnum.ProcessZamza)
        {
            return;
        }
        
        Throw();
    }

    private static void Throw()
    {
        throw new InvalidOperationException("Consumption controller's state is corrupted");
    }
}