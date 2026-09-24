#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <mutex>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstring>
#include <chrono>
#include <csignal>
#include <sstream>
#include <algorithm>
#include <atomic>

class ChatServer {
private:
    int server_fd;
    int port;
    std::map<int, std::string> clients; // socketfd -> username
    std::mutex clients_mutex;
    std::atomic<bool> running{true};
    
    void handleClient(int client_socket);
    void broadcastMessage(const std::string& message, int exclude_fd = -1);
    void sendUserList(int client_socket);
    
public:
    ChatServer(int port = 8888);
    ~ChatServer();
    bool start();
    void stop();
};

ChatServer::ChatServer(int port) : server_fd(-1), port(port) {}

ChatServer::~ChatServer() {
    stop();
}

bool ChatServer::start() {
    // 创建socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        std::cerr << "Failed to create socket" << std::endl;
        return false;
    }
    
    // 设置socket选项 - 允许地址重用
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "setsockopt failed" << std::endl;
        return false;
    }
    
    // 绑定到所有网络接口
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;  // 重要：绑定到所有接口
    address.sin_port = htons(port);
    
    if (bind(server_fd, (sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed on port " << port << std::endl;
        return false;
    }
    
    // 监听
    if (listen(server_fd, 10) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return false;
    }
    
    // 显示服务器信息
    std::cout << "==========================================" << std::endl;
    std::cout << "聊天服务器已启动成功！" << std::endl;
    std::cout << "监听端口: " << port << std::endl;
    std::cout << "服务器绑定地址: 0.0.0.0" << std::endl;
    std::cout << "客户端连接命令: python chat_client_fixed.py" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << "等待客户端连接..." << std::endl;
    
    // 接受客户端连接
    while (running) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_socket = accept(server_fd, (sockaddr*)&client_addr, &client_len);
        
        if (client_socket < 0) {
            std::cerr << "Accept failed" << std::endl;
            continue;
        }
        
        // 为新客户端创建线程
        std::thread client_thread(&ChatServer::handleClient, this, client_socket);
        client_thread.detach();
        
        std::cout << "[连接] 新客户端连接: " << inet_ntoa(client_addr.sin_addr) 
                  << ":" << ntohs(client_addr.sin_port) 
                  << " (socket: " << client_socket << ")" << std::endl;
    }
    
    return true;
}

void ChatServer::handleClient(int client_socket) {
    char buffer[1024];
    std::string username;
    
    std::cout << "[处理] 开始处理客户端 socket: " << client_socket << std::endl;
    
    while (running) {
        // 清空buffer
        memset(buffer, 0, sizeof(buffer));
        
        // 接收消息 - 使用MSG_DONTWAIT实现非阻塞
        int bytes_received = recv(client_socket, buffer, sizeof(buffer) - 1, MSG_DONTWAIT);
        
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0';
            std::string message(buffer);
            
            // 打印接收到的原始消息
            std::cout << "======================================" << std::endl;
            std::cout << "[接收] socket " << client_socket << " 收到消息: " << message << std::endl;
            std::cout << "消息长度: " << bytes_received << " 字节" << std::endl;
            
            // 解析消息
            std::istringstream iss(message);
            std::string msg_type, sender, receiver, content, timestamp;
            
            // 尝试解析各个字段
            if (std::getline(iss, msg_type, '|') &&
                std::getline(iss, sender, '|') &&
                std::getline(iss, receiver, '|') &&
                std::getline(iss, content, '|') &&
                std::getline(iss, timestamp, '|')) {
                
                std::cout << "[解析] 类型: " << msg_type 
                          << " | 发送者: " << sender 
                          << " | 接收者: " << receiver 
                          << " | 内容: " << content << std::endl;
                
                if (msg_type == "LOGIN") {
                    // 用户登录
                    {
                        std::lock_guard<std::mutex> lock(clients_mutex);
                        clients[client_socket] = sender;
                        username = sender;
                    }
                    
                    // 广播用户上线消息
                    std::string login_msg = "SYSTEM|SERVER|ALL|" + sender + " 进入了聊天室|" + timestamp;
                    std::cout << "[登录] 用户 " << sender << " 登录成功" << std::endl;
                    
                    // 先发送用户列表，再广播上线消息
                    sendUserList(client_socket);
                    broadcastMessage(login_msg, client_socket);
                    
                } else if (msg_type == "MESSAGE") {
                    // 转发消息
                    if (receiver == "ALL") {
                        // 广播消息
                        std::cout << "[广播] 准备广播消息给所有人" << std::endl;
                        broadcastMessage(message, client_socket);
                        std::cout << "[广播] 用户 " << sender << " -> 所有人: " << content << std::endl;
                    } else {
                        // 私聊消息
                        std::cout << "[私聊] 查找用户: " << receiver << std::endl;
                        std::lock_guard<std::mutex> lock(clients_mutex);
                        bool user_found = false;
                        for (const auto& client : clients) {
                            if (client.second == receiver) {
                                std::cout << "[私聊] 发送给用户 " << receiver 
                                          << " (socket: " << client.first << ")" << std::endl;
                                send(client.first, message.c_str(), message.length(), 0);
                                user_found = true;
                                std::cout << "[私聊] " << sender << " -> " << receiver << ": " << content << std::endl;
                                break;
                            }
                        }
                        if (!user_found) {
                            std::cout << "[私聊] 用户 " << receiver << " 不在线" << std::endl;
                            // 可以给发送者一个反馈
                            std::string error_msg = "SYSTEM|SERVER|" + sender + "|用户 " + receiver + " 不在线|" + timestamp;
                            send(client_socket, error_msg.c_str(), error_msg.length(), 0);
                        }
                    }
                } else if (msg_type == "LOGOUT") {
                    std::cout << "[退出] 用户请求退出: " << sender << std::endl;
                    break;
                }
            } else {
                std::cout << "[错误] 消息解析失败: " << message << std::endl;
            }
            
            std::cout << "======================================" << std::endl;
            
        } else if (bytes_received == 0) {
            // 连接关闭
            std::cout << "[断开] 客户端 socket " << client_socket << " 断开连接" << std::endl;
            break;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // 没有数据可读，非阻塞读取的正常情况
            // 短暂休眠避免CPU占用过高
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        } else {
            // 其他错误
            std::cerr << "[错误] recv 错误, errno: " << errno << std::endl;
            break;
        }
    }
    
    // 用户断开连接
    if (!username.empty()) {
        std::lock_guard<std::mutex> lock(clients_mutex);
        clients.erase(client_socket);
        
        // 广播用户下线消息
        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
            now.time_since_epoch()).count();
        std::string logout_msg = "SYSTEM|SERVER|ALL|" + username + " 离开了聊天室|" + std::to_string(timestamp);
        broadcastMessage(logout_msg);
        
        std::cout << "[退出] 用户 " << username << " 已退出" << std::endl;
    } else {
        std::cout << "[退出] 匿名客户端 socket " << client_socket << " 已退出" << std::endl;
    }
    
    close(client_socket);
}

void ChatServer::broadcastMessage(const std::string& message, int exclude_fd) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    
    std::cout << "=== 开始广播消息 ===" << std::endl;
    std::cout << "消息内容: " << message << std::endl;
    std::cout << "排除的客户端: " << exclude_fd << std::endl;
    std::cout << "当前在线客户端数量: " << clients.size() << std::endl;
    
    int broadcast_count = 0;
    std::string msg_with_newline = message + "\n";  // 添加换行符，方便客户端分割
    
    for (const auto& client : clients) {
        if (client.first != exclude_fd) {
            std::cout << "正在发送给用户: " << client.second 
                      << " (socket: " << client.first << ")" << std::endl;
            
            int bytes_sent = send(client.first, msg_with_newline.c_str(), msg_with_newline.length(), 0);
            
            if (bytes_sent < 0) {
                std::cerr << "发送失败给: " << client.second 
                          << "，错误码: " << errno << std::endl;
            } else {
                std::cout << "成功发送 " << bytes_sent << " 字节给: " 
                          << client.second << std::endl;
                broadcast_count++;
            }
        } else {
            std::cout << "跳过发送者自己: " << client.second << std::endl;
        }
    }
    
    std::cout << "=== 广播完成，共发送给 " << broadcast_count << " 个客户端 ===" << std::endl;
}

void ChatServer::sendUserList(int client_socket) {
    std::lock_guard<std::mutex> lock(clients_mutex);
    
    std::string user_list = "USERLIST|SERVER|";
    int user_count = 0;
    
    for (const auto& client : clients) {
        user_list += client.second + ",";
        user_count++;
    }
    
    if (!user_list.empty() && user_list.back() == ',') {
        user_list.pop_back(); // 移除最后一个逗号
    }
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch()).count();
    user_list += "|" + std::to_string(timestamp);
    
    // 添加换行符
    std::string user_list_with_newline = user_list + "\n";
    
    send(client_socket, user_list_with_newline.c_str(), user_list_with_newline.length(), 0);
    
    std::cout << "[用户列表] 发送给: " << clients[client_socket] 
              << "，在线用户: " << (clients.size() - 1) << "人" << std::endl;
    std::cout << "列表内容: " << user_list << std::endl;
}

void ChatServer::stop() {
    running = false;
    if (server_fd != -1) {
        close(server_fd);
        server_fd = -1;
    }
}

// 信号处理函数
void signalHandler(int signum) {
    std::cout << "\n[服务器] 收到信号 " << signum << "，正在关闭服务器..." << std::endl;
    exit(signum);
}

int main() {
    // 注册信号处理
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    
    std::cout << "正在启动聊天服务器..." << std::endl;
    
    ChatServer server(8888);
    
    if (!server.start()) {
        std::cerr << "服务器启动失败!" << std::endl;
        return 1;
    }
    
    return 0;
}