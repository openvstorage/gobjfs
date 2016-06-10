
// server side structures
NetworkXioRequest 
{
}

NetworkClientData is server-side per-connection data
{
}

NetworkXioIOHandler is server-side per-connection request handler
{
}

NetworkXioServer loop
{
  NetworkXioServer s
  s->run()
  {
    on_session_event -> 
      set connection attribute O_CONNECTION_ATTR_USER_CTX 
      to be NetworkXioClientData 
      The ClientData internally has NetworkXioIOHandler

    for data on individual socket ->
      call NetworkXioHandler::handle_request()
  }
}
