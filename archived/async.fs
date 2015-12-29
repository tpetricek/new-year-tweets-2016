module AsyncHelpers
#nowarn "40"
open System

// --------------------------------------------------------------------------------------
// Agents for handling tweet buffering and synchronization
// --------------------------------------------------------------------------------------

/// Groups the incoming events into batches and produces batches when either the
/// specified number of items is available or when nothing happens in a specified time
type QueueAgent<'T>(maxCount:int, timeout:int) = 
  let event = Event<list<'T>>()
  let error = Event<_>()
  let agent = MailboxProcessor.Start(fun inbox -> 
    let rec loop (lastBatch:DateTime) count events = async {
      let elapsed = int (DateTime.UtcNow - lastBatch).TotalMilliseconds
      let sleep = max 10 (timeout - elapsed)
      printfn "Sleep: %A" sleep
      let! evt = inbox.TryReceive(sleep)
      match evt with
      | None when count = 0 -> 
          return! loop lastBatch 0 []
      | None -> 
          try event.Trigger(events)
          with e -> error.Trigger(e)
          return! loop DateTime.UtcNow 0 []
      | Some(e) when count = maxCount - 1 ->
          try event.Trigger(e::events)
          with e -> error.Trigger(e)
          return! loop DateTime.UtcNow 0 []
      | Some(e) ->
          return! loop lastBatch (count+1) (e::events) }
    loop DateTime.UtcNow 0 [] )

  /// A batch of events has been produced
  member x.BatchOccurred = event.Publish
  /// Send a new event to the batching agent
  member x.AddEvent(event) = agent.Post(event)
  /// Exception has been thrown when triggering `EventOccurred`
  member x.ErrorOccurred = Event.merge agent.Error error.Publish

// --------------------------------------------------------------------------------------
// Expose functionality as 'Observable' module extensions
// --------------------------------------------------------------------------------------

module Observable =
  let private observable f = 
    { new IObservable<_> with
        member x.Subscribe(obs) = f obs }
  
  /// Groups the incoming events into batches and produces batches when either the
  /// specified number of items is available or when nothing happens in a specified time
  let batch count timeout (source:IObservable<_>) =
    observable (fun obs ->
      let batch = QueueAgent<_>(count, timeout)
      batch.BatchOccurred.Add(obs.OnNext)
      batch.ErrorOccurred.Add(obs.OnError)
      { new IObserver<_> with
          member x.OnCompleted() = obs.OnCompleted()
          member x.OnError(e) = obs.OnError(e)
          member x.OnNext(v) = batch.AddEvent(v) }
      |> source.Subscribe )
