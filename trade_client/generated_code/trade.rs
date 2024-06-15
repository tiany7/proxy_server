#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAggTradeRequest {
    #[prost(string, tag = "1")]
    pub symbol: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggTradeData {
    #[prost(string, tag = "1")]
    pub symbol: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub is_buyer_maker: bool,
    #[prost(double, tag = "3")]
    pub price: f64,
    #[prost(double, tag = "4")]
    pub quantity: f64,
    #[prost(string, tag = "5")]
    pub event_type: ::prost::alloc::string::String,
    #[prost(uint64, tag = "6")]
    pub first_break_trade_id: u64,
    #[prost(uint64, tag = "7")]
    pub last_break_trade_id: u64,
    #[prost(uint64, tag = "8")]
    pub aggregated_trade_id: u64,
    #[prost(uint64, tag = "9")]
    pub trade_time: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetAggTradeResponse {
    #[prost(message, optional, tag = "1")]
    pub data: ::core::option::Option<AggTradeData>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetHeartbeatRequest {
    #[prost(string, tag = "1")]
    pub ping: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetHeartbeatResponse {
    #[prost(string, tag = "1")]
    pub metrics: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetMarketDataRequest {
    #[prost(string, tag = "1")]
    pub symbol: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub granularity: ::core::option::Option<TimeDuration>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeDuration {
    #[prost(int64, tag = "1")]
    pub value: i64,
    #[prost(enumeration = "TimeUnit", tag = "2")]
    pub unit: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetMarketDataResponse {
    #[prost(message, optional, tag = "1")]
    pub data: ::core::option::Option<BarData>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BarData {
    #[prost(uint64, tag = "1")]
    pub open_time: u64,
    #[prost(double, tag = "2")]
    pub open: f64,
    #[prost(double, tag = "3")]
    pub high: f64,
    #[prost(double, tag = "4")]
    pub low: f64,
    #[prost(double, tag = "5")]
    pub close: f64,
    #[prost(double, tag = "6")]
    pub volume: f64,
    #[prost(uint64, tag = "7")]
    pub close_time: u64,
    #[prost(double, tag = "8")]
    pub quote_asset_volume: f64,
    #[prost(uint64, tag = "9")]
    pub number_of_trades: u64,
    #[prost(double, tag = "10")]
    pub taker_buy_base_asset_volume: f64,
    #[prost(double, tag = "11")]
    pub taker_buy_quote_asset_volume: f64,
    #[prost(uint64, tag = "12")]
    pub missing_count: u64,
    #[prost(uint64, tag = "13")]
    pub max_id: u64,
    #[prost(uint64, tag = "14")]
    pub min_id: u64,
}
/// for utility in server, not for rpc
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Column {
    #[prost(string, tag = "1")]
    pub column_name: ::prost::alloc::string::String,
    #[prost(oneof = "column::Data", tags = "3, 4, 5, 6")]
    pub data: ::core::option::Option<column::Data>,
}
/// Nested message and enum types in `Column`.
pub mod column {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        #[prost(string, tag = "3")]
        StringValue(::prost::alloc::string::String),
        #[prost(uint64, tag = "4")]
        UintValue(u64),
        #[prost(bool, tag = "5")]
        BoolValue(bool),
        #[prost(double, tag = "6")]
        DoubleValue(f64),
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Minutes = 0,
    Seconds = 1,
    Milliseconds = 2,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Minutes => "MINUTES",
            TimeUnit::Seconds => "SECONDS",
            TimeUnit::Milliseconds => "MILLISECONDS",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MINUTES" => Some(Self::Minutes),
            "SECONDS" => Some(Self::Seconds),
            "MILLISECONDS" => Some(Self::Milliseconds),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod trade_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
use std::convert::TryInto;

    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct TradeClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl TradeClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> TradeClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> TradeClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            TradeClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn get_agg_trade_stream(
            &mut self,
            request: impl tonic::IntoRequest<super::GetAggTradeRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::GetAggTradeResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/trade.Trade/GetAggTradeStream",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("trade.Trade", "GetAggTradeStream"));
            self.inner.server_streaming(req, path, codec).await
        }
        pub async fn get_client_heartbeat(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::GetHeartbeatRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::GetHeartbeatResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/trade.Trade/GetClientHeartbeat",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("trade.Trade", "GetClientHeartbeat"));
            self.inner.streaming(req, path, codec).await
        }
        pub async fn get_market_data(
            &mut self,
            request: impl tonic::IntoRequest<super::GetMarketDataRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::GetMarketDataResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/trade.Trade/GetMarketData",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("trade.Trade", "GetMarketData"));
            self.inner.server_streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod trade_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with TradeServer.
    #[async_trait]
    pub trait Trade: Send + Sync + 'static {
        /// Server streaming response type for the GetAggTradeStream method.
        type GetAggTradeStreamStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::GetAggTradeResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn get_agg_trade_stream(
            &self,
            request: tonic::Request<super::GetAggTradeRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::GetAggTradeStreamStream>,
            tonic::Status,
        >;
        /// Server streaming response type for the GetClientHeartbeat method.
        type GetClientHeartbeatStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::GetHeartbeatResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn get_client_heartbeat(
            &self,
            request: tonic::Request<tonic::Streaming<super::GetHeartbeatRequest>>,
        ) -> std::result::Result<
            tonic::Response<Self::GetClientHeartbeatStream>,
            tonic::Status,
        >;
        /// Server streaming response type for the GetMarketData method.
        type GetMarketDataStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::GetMarketDataResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn get_market_data(
            &self,
            request: tonic::Request<super::GetMarketDataRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::GetMarketDataStream>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct TradeServer<T: Trade> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Trade> TradeServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for TradeServer<T>
    where
        T: Trade,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/trade.Trade/GetAggTradeStream" => {
                    #[allow(non_camel_case_types)]
                    struct GetAggTradeStreamSvc<T: Trade>(pub Arc<T>);
                    impl<
                        T: Trade,
                    > tonic::server::ServerStreamingService<super::GetAggTradeRequest>
                    for GetAggTradeStreamSvc<T> {
                        type Response = super::GetAggTradeResponse;
                        type ResponseStream = T::GetAggTradeStreamStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetAggTradeRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Trade>::get_agg_trade_stream(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetAggTradeStreamSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/trade.Trade/GetClientHeartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct GetClientHeartbeatSvc<T: Trade>(pub Arc<T>);
                    impl<
                        T: Trade,
                    > tonic::server::StreamingService<super::GetHeartbeatRequest>
                    for GetClientHeartbeatSvc<T> {
                        type Response = super::GetHeartbeatResponse;
                        type ResponseStream = T::GetClientHeartbeatStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::GetHeartbeatRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Trade>::get_client_heartbeat(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetClientHeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/trade.Trade/GetMarketData" => {
                    #[allow(non_camel_case_types)]
                    struct GetMarketDataSvc<T: Trade>(pub Arc<T>);
                    impl<
                        T: Trade,
                    > tonic::server::ServerStreamingService<super::GetMarketDataRequest>
                    for GetMarketDataSvc<T> {
                        type Response = super::GetMarketDataResponse;
                        type ResponseStream = T::GetMarketDataStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetMarketDataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Trade>::get_market_data(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetMarketDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Trade> Clone for TradeServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Trade> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Trade> tonic::server::NamedService for TradeServer<T> {
        const NAME: &'static str = "trade.Trade";
    }
}
