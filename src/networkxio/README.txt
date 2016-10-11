CLIENT SIDE

struct ovs_context_t 
{
  NetworkXioClient net_client_
}

volumedriver wrapper calls ovs_send
{
  NetworkXioClient->xio_send_read_request()
  {
    push into inflight_reqs queue
  }
}

another thread runs NetworkXioClient::xio_run_loop_worker()
{
  xio_context_run_loop()

  while (pop from inflight_reqs queue)
  {
    xio_send_request
  }
}

==========================

aio_request {
}

NetworkXioMsg {
    opaque -> ptr to xio_msg_s
}

xio_msg_s {
    xio_msg xreq -> sent to client
    NetworkXioMsg msg
    std::string s_msg -> 
    opaque -> ptr to aio_request
}

xio_msg {
}

NetworkXioRequest {
}

gIOBatch {
}

==========================

SERVER SIDE 

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
