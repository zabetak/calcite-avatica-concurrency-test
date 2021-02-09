# calcite-avatica-concurrency-test

Illustrates Avatica JDBC Concurrency problem

    https://issues.apache.org/jira/browse/CALCITE-4489

- Starts Avatica http server to services calcite csv example over the wire.
- Runs multiple (20) clients to read data over apache remote protocol.

 With some 'luck' expected to see this error

```
java.lang.AssertionError: null
    at org.apache.calcite.jdbc.CalciteConnectionImpl$CalciteServerImpl.addStatement(CalciteConnectionImpl.java:364)
    at org.apache.calcite.jdbc.CalciteMetaImpl.createStatement(CalciteMetaImpl.java:145)
    at org.apache.calcite.avatica.remote.LocalService.apply(LocalService.java:271)
    at org.apache.calcite.avatica.remote.Service$CreateStatementRequest.accept(Service.java:1514)
    at org.apache.calcite.avatica.remote.Service$CreateStatementRequest.accept(Service.java:1497)
    at org.apache.calcite.avatica.remote.AbstractHandler.apply(AbstractHandler.java:94)
    at org.apache.calcite.avatica.remote.JsonHandler.apply(JsonHandler.java:52)
    at org.apache.calcite.avatica.server.AvaticaJsonHandler.handle(AvaticaJsonHandler.java:133)
    at org.eclipse.jetty.server.handler.HandlerList.handle(HandlerList.java:61)
    at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:132)
    at org.eclipse.jetty.server.Server.handle(Server.java:502)
    at org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:370)
    at org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:267)
    at org.eclipse.jetty.io.AbstractConnection$ReadCallback.succeeded(AbstractConnection.java:305)
    at org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:103)
    at org.eclipse.jetty.io.ChannelEndPoint$2.run(ChannelEndPoint.java:117)
    at org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.runTask(EatWhatYouKill.java:333)
    at org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.doProduce(EatWhatYouKill.java:310)
    at org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.tryProduce(EatWhatYouKill.java:168)
    at org.eclipse.jetty.util.thread.strategy.EatWhatYouKill.run(EatWhatYouKill.java:126)
    at org.eclipse.jetty.util.thread.ReservedThreadExecutor$ReservedThread.run(ReservedThreadExecutor.java:366)
    at org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:765)
    at org.eclipse.jetty.util.thread.QueuedThreadPool$2.run(QueuedThreadPool.java:683)
    at java.lang.Thread.run(Thread.java:748
```
