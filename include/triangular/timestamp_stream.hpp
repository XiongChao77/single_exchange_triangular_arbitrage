#pragma once

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket/teardown.hpp>
#include <nlohmann/json.hpp>
#include <linux/errqueue.h>
#include <linux/net_tstamp.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <array>
#include <cerrno>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

namespace triangular {
inline std::int64_t realtime_ns() {
    timespec value{}; clock_gettime(CLOCK_REALTIME, &value);
    return std::int64_t(value.tv_sec)*1000000000 + value.tv_nsec;
}

struct KernelRxBatch {std::uint64_t serial{},begin{},end{};std::int64_t timestamp{};};
struct KernelRxWindow {
    std::array<KernelRxBatch,64> batches{};
    std::size_t count{};
    bool no_new_read{},history_truncated{};
    std::uint64_t missing{},control_truncated{};
    nlohmann::json json() const {
        auto records=nlohmann::json::array();
        for(std::size_t i=0;i<count;++i) {
            const auto& item=batches[i];
            records.push_back({{"read_id",item.serial},{"ciphertext_begin",item.begin},{"ciphertext_end",item.end},
                {"kernel_rx_realtime_ns",item.timestamp?nlohmann::json(item.timestamp):nlohmann::json(nullptr)}});
        }
        return {{"scope","tcp_reads_during_websocket_read"},{"exact_message_mapping",false},
            {"no_new_transport_read",no_new_read},{"history_truncated",history_truncated},
            {"missing_rx_timestamps_total",missing},{"control_truncated_total",control_truncated},
            {"batches",std::move(records)}};
    }
};

// Capture ancillary metadata below TLS. RX metadata describes a TCP read batch,
// never an exact WebSocket-message boundary. No payload or TLS secrets are logged.
class TimestampStream {
    boost::beast::tcp_stream stream_;
    bool rx_enabled_ = false;
    bool tx_enabled_ = false;
    int timestamp_error_ = 0;
    std::uint64_t rx_reads_ = 0, rx_bytes_ = 0, tx_bytes_ = 0;
    std::vector<KernelRxBatch> rx_;
    std::uint64_t rx_missing_ = 0, control_truncated_ = 0;

    template<class Buffers>
    std::size_t receive(const Buffers& buffers, boost::system::error_code& error) {
        std::array<iovec, 64> vectors{};
        std::size_t count=0;
        for(auto it=boost::asio::buffer_sequence_begin(buffers), end=boost::asio::buffer_sequence_end(buffers);
            it!=end && count<vectors.size(); ++it) {
            boost::asio::mutable_buffer buffer(*it);
            if(buffer.size()) vectors[count++]={buffer.data(),buffer.size()};
        }
        if(!count) {error.clear(); return 0;}
        alignas(cmsghdr) std::array<char,512> control{};
        msghdr msg{}; msg.msg_iov=vectors.data(); msg.msg_iovlen=count;
        msg.msg_control=control.data(); msg.msg_controllen=control.size();
        const auto bytes=::recvmsg(socket().native_handle(),&msg,MSG_DONTWAIT);
        if(bytes<0) {error={errno,boost::system::generic_category()}; return 0;}
        if(!bytes) {error=boost::asio::error::eof; return 0;}
        error.clear();
        std::int64_t timestamp=0;
        if(msg.msg_flags&MSG_CTRUNC) ++control_truncated_;
        for(auto* c=CMSG_FIRSTHDR(&msg); c; c=CMSG_NXTHDR(&msg,c))
            if(c->cmsg_level==SOL_SOCKET && c->cmsg_type==SCM_TIMESTAMPING && c->cmsg_len>=CMSG_LEN(sizeof(timespec)*3)) {
                timespec values[3]{}; std::memcpy(values,CMSG_DATA(c),sizeof(values));
                timestamp=std::int64_t(values[0].tv_sec)*1000000000+values[0].tv_nsec;
            }
        if(rx_enabled_) {
            if(!timestamp) ++rx_missing_;
            if(rx_.size()==64) rx_.erase(rx_.begin());
            rx_.push_back({++rx_reads_,rx_bytes_,rx_bytes_+static_cast<std::uint64_t>(bytes),timestamp});
        }
        rx_bytes_+=static_cast<std::uint64_t>(bytes);
        return static_cast<std::size_t>(bytes);
    }
public:
    using executor_type=boost::beast::tcp_stream::executor_type;
    using lowest_layer_type=boost::asio::ip::tcp::socket;
    lowest_layer_type& lowest_layer() noexcept {return stream_.socket();}
    const lowest_layer_type& lowest_layer() const noexcept {return stream_.socket();}
    template<class Executor> explicit TimestampStream(Executor&& executor):stream_(std::forward<Executor>(executor)) {}
    executor_type get_executor() noexcept {return stream_.get_executor();}
    boost::beast::tcp_stream& next_layer() noexcept {return stream_;}
    const boost::beast::tcp_stream& next_layer() const noexcept {return stream_;}
    auto& socket() noexcept {return stream_.socket();}

    void enable(bool rx, bool tx) {
        if(!rx && !tx) return;
        int flags=SOF_TIMESTAMPING_SOFTWARE;
        if(rx) flags|=SOF_TIMESTAMPING_RX_SOFTWARE;
        if(tx) flags|=SOF_TIMESTAMPING_TX_SOFTWARE|SOF_TIMESTAMPING_TX_SCHED|
            SOF_TIMESTAMPING_OPT_ID|SOF_TIMESTAMPING_OPT_ID_TCP|SOF_TIMESTAMPING_OPT_TSONLY;
        if(::setsockopt(socket().native_handle(),SOL_SOCKET,SO_TIMESTAMPING,&flags,sizeof(flags))<0) {
            timestamp_error_=errno; return;
        }
        rx_enabled_=rx; tx_enabled_=tx;
    }
    nlohmann::json capability() const {
        return {{"rx_enabled",rx_enabled_},{"tx_enabled",tx_enabled_},{"setsockopt_errno",timestamp_error_},
            {"clock","CLOCK_REALTIME"},{"rx_scope","tcp_recvmsg_batch"}};
    }
    std::uint64_t rx_reads() const {return rx_reads_;}
    std::uint64_t tx_bytes() const {return tx_bytes_;}
    KernelRxWindow rx_window(std::uint64_t before) const {
        KernelRxWindow window;
        window.no_new_read=rx_reads_==before;window.history_truncated=rx_reads_-before>64;
        window.missing=rx_missing_;window.control_truncated=control_truncated_;
        for(const auto& item:rx_) if(item.serial>before) window.batches[window.count++]=item;
        return window;
    }
    nlohmann::json drain_tx(std::uint64_t begin,std::uint64_t end) {
        auto records=nlohmann::json::array();
        std::int64_t final_software=0, final_sched=0;
        unsigned drained=0;
        bool truncated=false;
        if(tx_enabled_) for(;drained<256;++drained) {
            alignas(cmsghdr) std::array<char,512> control{};
            char payload{}; iovec vector{&payload,1}; msghdr msg{};
            msg.msg_iov=&vector; msg.msg_iovlen=1; msg.msg_control=control.data(); msg.msg_controllen=control.size();
            if(::recvmsg(socket().native_handle(),&msg,MSG_ERRQUEUE|MSG_DONTWAIT)<0) break;
            if(msg.msg_flags&MSG_CTRUNC) truncated=true;
            timespec stamp{}; sock_extended_err info{}; bool has_info=false;
            for(auto* c=CMSG_FIRSTHDR(&msg);c;c=CMSG_NXTHDR(&msg,c)) {
                if(c->cmsg_level==SOL_SOCKET && c->cmsg_type==SCM_TIMESTAMPING && c->cmsg_len>=CMSG_LEN(sizeof(timespec)*3))
                    std::memcpy(&stamp,CMSG_DATA(c),sizeof(stamp));
                if(((c->cmsg_level==SOL_IP && c->cmsg_type==IP_RECVERR) ||
                    (c->cmsg_level==SOL_IPV6 && c->cmsg_type==IPV6_RECVERR)) && c->cmsg_len>=CMSG_LEN(sizeof(info))) {
                    std::memcpy(&info,CMSG_DATA(c),sizeof(info)); has_info=true;
                }
            }
            if(!has_info || info.ee_origin!=SO_EE_ORIGIN_TIMESTAMPING || info.ee_errno!=ENOMSG || end<=begin) continue;
            // TCP IDs wrap at 32 bits. A single request must be smaller than 2^32 bytes.
            const auto offset=std::uint32_t(info.ee_data-std::uint32_t(begin));
            if(offset>=end-begin) continue;
            const auto ns=std::int64_t(stamp.tv_sec)*1000000000+stamp.tv_nsec;
            if(!ns) continue;
            const auto stage=info.ee_info==SCM_TSTAMP_SCHED ? "TX_SCHED" :
                info.ee_info==SCM_TSTAMP_SND ? "TX_SOFTWARE" : "OTHER";
            records.push_back({{"byte_id",info.ee_data},{"stage",stage},{"realtime_ns",ns}});
            if(info.ee_data==std::uint32_t(end-1)) {
                if(info.ee_info==SCM_TSTAMP_SND) final_software=ns;
                if(info.ee_info==SCM_TSTAMP_SCHED) final_sched=ns;
            }
        }
        return {{"enabled",tx_enabled_},{"setsockopt_errno",timestamp_error_},{"ciphertext_begin",begin},{"ciphertext_end",end},
            {"kernel_tx_software_realtime_ns",final_software?nlohmann::json(final_software):nlohmann::json(nullptr)},
            {"kernel_tx_sched_realtime_ns",final_sched?nlohmann::json(final_sched):nlohmann::json(nullptr)},
            {"final_byte_timestamp_present",final_software!=0},{"drain_limit_reached",drained==256},
            {"control_truncated",truncated},{"records",std::move(records)}};
    }
    template<class Buffers> std::size_t read_some(const Buffers& buffers,boost::system::error_code& error) {
        if(!rx_enabled_) return stream_.read_some(buffers,error);
        for(;;) {
            const auto result=receive(buffers,error);
            if(error.value()!=EAGAIN && error.value()!=EWOULDBLOCK && error.value()!=EINTR) return result;
            pollfd descriptor{socket().native_handle(),POLLIN,0};
            int ready; do {ready=::poll(&descriptor,1,10000);} while(ready<0 && errno==EINTR);
            if(ready<=0) {error=ready==0?boost::asio::error::timed_out:boost::system::error_code(errno,boost::system::generic_category());return 0;}
        }
    }
    template<class Buffers> std::size_t read_some(const Buffers& buffers) {
        boost::system::error_code error; auto bytes=read_some(buffers,error); if(error) throw boost::system::system_error(error); return bytes;
    }
    template<class Buffers,class Handler> void async_read_some(const Buffers& buffers,Handler&& handler) {
        if(!rx_enabled_) {stream_.async_read_some(buffers,std::forward<Handler>(handler));return;}
        using H=std::decay_t<Handler>;
        struct Operation:std::enable_shared_from_this<Operation> {
            TimestampStream& stream; Buffers buffers; H handler;
            Operation(TimestampStream& s,const Buffers& b,H h):stream(s),buffers(b),handler(std::move(h)) {}
            void attempt(boost::system::error_code error = {}) {
                if(error) {handler(error,0);return;}
                const auto bytes=stream.receive(buffers,error);
                if(error.value()==EAGAIN || error.value()==EWOULDBLOCK || error.value()==EINTR) {
                    stream.socket().async_wait(boost::asio::ip::tcp::socket::wait_read,
                        [self=this->shared_from_this()](boost::system::error_code wait_error) {self->attempt(wait_error);});
                } else handler(error,bytes);
            }
        };
        auto operation=std::make_shared<Operation>(*this,buffers,std::forward<Handler>(handler));
        boost::asio::post(get_executor(),[operation]{operation->attempt();});
    }
    template<class Buffers> std::size_t write_some(const Buffers& buffers,boost::system::error_code& error) {
        auto bytes=stream_.write_some(buffers,error);tx_bytes_+=bytes;return bytes;
    }
    template<class Buffers> std::size_t write_some(const Buffers& buffers) {
        boost::system::error_code error;auto bytes=write_some(buffers,error);if(error) throw boost::system::system_error(error);return bytes;
    }
    template<class Buffers,class Handler> void async_write_some(const Buffers& buffers,Handler&& handler) {
        stream_.async_write_some(buffers,[this,handler=std::forward<Handler>(handler)](boost::system::error_code error,std::size_t bytes) mutable {
            tx_bytes_+=bytes;handler(error,bytes);
        });
    }
};
inline void teardown(boost::beast::role_type role,TimestampStream& stream,boost::system::error_code& error) {
    using boost::beast::websocket::teardown;teardown(role,stream.next_layer(),error);
}
template<class Handler> void async_teardown(boost::beast::role_type role,TimestampStream& stream,Handler&& handler) {
    using boost::beast::websocket::async_teardown;async_teardown(role,stream.next_layer(),std::forward<Handler>(handler));
}
} // namespace triangular
