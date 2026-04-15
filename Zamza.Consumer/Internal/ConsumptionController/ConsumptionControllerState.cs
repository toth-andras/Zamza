namespace Zamza.Consumer.Internal.ConsumptionController;

internal sealed class ConsumptionControllerState
{
    public ConsumptionControllerStateEnum CurrentState { get; private set; }

    public ConsumptionControllerState()
    {
        CurrentState = ConsumptionControllerStateEnum.ProcessKafka;
    }

    public void ChangeState(ConsumptionControllerStateEnum newState)
    {
        switch (CurrentState)
        {
            case ConsumptionControllerStateEnum.Stopped:
                ValidateForStopped(newState);
                break;
            
            case ConsumptionControllerStateEnum.ZamzaServerNotAvailable:
                ValidateForZamzaServerNotAvailable(newState);
                break;
            
            case ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired:
                ValidateForPartitionOwnershipClaimRequired(newState);
                break;
            
            case ConsumptionControllerStateEnum.ProcessKafka:
                ValidateForProcessKafka(newState);
                break;
            
            case ConsumptionControllerStateEnum.ProcessZamza:
                ValidateForProcessZamza(newState);
                break;
        }
        
        CurrentState = newState;
    }

    private void ValidateForStopped(ConsumptionControllerStateEnum state)
    {
        if (state is not ConsumptionControllerStateEnum.Stopped)
        {
            Throw();
        }
    }
    private void ValidateForZamzaServerNotAvailable(ConsumptionControllerStateEnum state)
    {
        if (state is 
            ConsumptionControllerStateEnum.Stopped or 
            ConsumptionControllerStateEnum.ZamzaServerNotAvailable or 
            ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForPartitionOwnershipClaimRequired(ConsumptionControllerStateEnum state)
    {
        if (state is 
            ConsumptionControllerStateEnum.Stopped or
            ConsumptionControllerStateEnum.ZamzaServerNotAvailable or
            ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired or
            ConsumptionControllerStateEnum.ProcessKafka or
            ConsumptionControllerStateEnum.ProcessZamza)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForProcessKafka(ConsumptionControllerStateEnum state)
    {
        if (state is
            ConsumptionControllerStateEnum.Stopped or
            ConsumptionControllerStateEnum.ZamzaServerNotAvailable or
            ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired or
            ConsumptionControllerStateEnum.ProcessKafka or
            ConsumptionControllerStateEnum.ProcessZamza)
        {
            return;
        }
        
        Throw();
    }
    private void ValidateForProcessZamza(ConsumptionControllerStateEnum state)
    {
        if (state is
            ConsumptionControllerStateEnum.Stopped or
            ConsumptionControllerStateEnum.ZamzaServerNotAvailable or
            ConsumptionControllerStateEnum.PartitionOwnershipClaimRequired or
            ConsumptionControllerStateEnum.ProcessKafka or
            ConsumptionControllerStateEnum.ProcessZamza)
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