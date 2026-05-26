use std::io::Error;
use std::net::SocketAddr;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

trait FromAsyncStream {
    type Error;
    async fn from_async_stream<IO>(stream: &mut IO) -> Result<Self, Self::Error>
    where
        IO: AsyncBufRead + Unpin,
        Self: Sized;
}

#[derive(Debug)]
pub struct GetRequest {
    pub filename: String,
}

#[derive(Debug)]
pub struct PutRequest {
    pub filename: String,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct DeleteRequest {
    pub filename: String,
}

pub enum Request {
    Get(GetRequest),
    Put(PutRequest),
    Delete(DeleteRequest),
    List,
}

#[derive(Debug)]
pub struct GetResponse(Vec<u8>);
#[derive(Debug)]
pub struct OkResponse;
#[derive(Debug)]
pub struct ListResponse(pub Vec<String>);

#[derive(Debug)]
pub enum RequestError {
    IoError(Error),
    ResponseError(String),
    ParseError,
}

impl From<Error> for RequestError {
    fn from(value: Error) -> Self {
        Self::IoError(value)
    }
}

#[derive(Debug)]
enum Response {
    Data(Vec<u8>),
    Empty,
    Error(String),
}

impl ToBytes for GetRequest {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(b"GET ");
        bytes.extend(self.filename.as_bytes());
        bytes.extend(b"\n");
        bytes
    }
}

impl ToBytes for PutRequest {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(b"PUT ");
        bytes.extend(self.filename.as_bytes());
        bytes.extend(b"\n");
        bytes.extend(&(self.data.len() as u64).to_le_bytes());
        bytes.extend(&self.data);
        bytes
    }
}

impl ToBytes for DeleteRequest {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(b"DELETE ");
        bytes.extend(self.filename.as_bytes());
        bytes.extend(b"\n");
        bytes
    }
}

impl FromAsyncStream for Response {
    type Error = RequestError;
    async fn from_async_stream<IO>(stream: &mut IO) -> Result<Self, RequestError>
    where
        IO: AsyncBufRead + Unpin,
        Self: Sized,
    {
        let mut buf = String::new();
        stream.read_line(&mut buf).await?;
        match buf.as_str() {
            "OK\n" => {
                // peek to distinguish EOF (no data follows) from a size+data payload
                if stream.fill_buf().await?.is_empty() {
                    return Ok(Response::Empty);
                }

                let mut size_buf = [0u8; 8];
                stream.read_exact(&mut size_buf).await?;
                let size = usize::try_from(u64::from_le_bytes(size_buf))
                    .map_err(|_| RequestError::ParseError)?;

                let mut data = vec![0; size];
                stream.read_exact(&mut data).await?;

                // the stream should now be closed
                if stream.read(&mut [0u8; 1]).await? != 0 {
                    return Err(RequestError::ParseError);
                }

                Ok(Response::Data(data))
            }
            "ERROR\n" => {
                let mut message = String::new();
                let bytes_read = stream.read_line(&mut message).await?;
                if bytes_read == 0 || !message.ends_with('\n') {
                    return Err(RequestError::ParseError);
                }
                message.pop();
                Ok(Response::Error(message))
            }
            _ => Err(RequestError::ParseError),
        }
    }
}

impl FromAsyncStream for GetResponse {
    type Error = RequestError;
    async fn from_async_stream<IO>(stream: &mut IO) -> Result<Self, RequestError>
    where
        IO: AsyncBufRead + Unpin,
        Self: Sized,
    {
        match Response::from_async_stream(stream).await? {
            Response::Data(data) => Ok(GetResponse(data)),
            Response::Empty => Err(RequestError::ParseError),
            Response::Error(message) => Err(RequestError::ResponseError(message)),
        }
    }
}

impl FromAsyncStream for OkResponse {
    type Error = RequestError;
    async fn from_async_stream<IO>(stream: &mut IO) -> Result<Self, RequestError>
    where
        IO: AsyncBufRead + Unpin,
        Self: Sized,
    {
        match Response::from_async_stream(stream).await? {
            Response::Empty => Ok(OkResponse),
            Response::Data(_) => Err(RequestError::ParseError),
            Response::Error(message) => Err(RequestError::ResponseError(message)),
        }
    }
}

impl FromAsyncStream for ListResponse {
    type Error = RequestError;
    async fn from_async_stream<IO>(stream: &mut IO) -> Result<Self, RequestError>
    where
        IO: AsyncBufRead + Unpin,
        Self: Sized,
    {
        match Response::from_async_stream(stream).await? {
            Response::Data(data) => {
                let text = String::from_utf8(data).map_err(|_| RequestError::ParseError)?;
                let files = if text.is_empty() {
                    Vec::new()
                } else {
                    text.split('\n').map(String::from).collect()
                };
                Ok(ListResponse(files))
            }
            Response::Empty => Ok(ListResponse(Vec::new())),
            Response::Error(message) => Err(RequestError::ResponseError(message)),
        }
    }
}

#[derive(Debug)]
pub struct Client {
    pub socket: SocketAddr,
}

impl Client {
    pub fn new(socket: SocketAddr) -> Client {
        Self { socket }
    }

    pub async fn get(&self, req: &GetRequest) -> Result<GetResponse, RequestError> {
        let mut conn = TcpStream::connect(self.socket).await?;
        conn.write_all(&req.to_bytes()).await?;
        let mut reader = BufReader::new(conn);
        GetResponse::from_async_stream(&mut reader).await
    }

    pub async fn put(&self, req: &PutRequest) -> Result<OkResponse, RequestError> {
        let mut conn = TcpStream::connect(self.socket).await?;
        conn.write_all(&req.to_bytes()).await?;
        let mut reader = BufReader::new(conn);
        OkResponse::from_async_stream(&mut reader).await
    }

    pub async fn delete(&self, req: &DeleteRequest) -> Result<OkResponse, RequestError> {
        let mut conn = TcpStream::connect(self.socket).await?;
        conn.write_all(&req.to_bytes()).await?;
        let mut reader = BufReader::new(conn);
        OkResponse::from_async_stream(&mut reader).await
    }

    pub async fn list(&self) -> Result<ListResponse, RequestError> {
        let mut conn = TcpStream::connect(self.socket).await?;
        conn.write_all(b"LIST\n").await?;
        let mut reader = BufReader::new(conn);
        ListResponse::from_async_stream(&mut reader).await
    }
}
