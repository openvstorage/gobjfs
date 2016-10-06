/*
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
*/

#include "NetworkXioCommon.h"
#include <arpa/inet.h>

namespace gobjfs {
namespace xio {

std::string getURI(xio_session* s) {
    xio_session_attr attr;
    int ret = xio_query_session(s, &attr, XIO_SESSION_ATTR_URI);
    if (ret == 0) {
	return attr.uri;
    }
    return "null";
}

int getAddressAndPort(xio_connection* conn, 
    std::string& localAddr, int& localPort,
    std::string& peerAddr, int& peerPort) {

  if (!conn) {
    return -1;
  }

  xio_connection_attr conn_attr;
  int ret = xio_query_connection(conn,
    &conn_attr,
    XIO_CONNECTION_ATTR_LOCAL_ADDR | XIO_CONNECTION_ATTR_PEER_ADDR);

  if (ret == 0) {
    {
      sockaddr_in *pa = (sockaddr_in*)&conn_attr.peer_addr;
      peerAddr = inet_ntoa(pa->sin_addr);
      peerPort = pa->sin_port;
    }
    {
      sockaddr_in *la = (sockaddr_in*)&conn_attr.local_addr;
      localAddr = inet_ntoa(la->sin_addr);
      localPort = la->sin_port;
    }
  } else {
    localAddr = peerAddr = "null";
    localPort = peerPort = -1;
  }
  return ret;
}

}
}
