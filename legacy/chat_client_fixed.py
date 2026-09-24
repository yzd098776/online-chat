#!/usr/bin/env python3
"""
在线聊天程序客户端 - 修复版
修复消息不可见问题，增强调试功能
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import socket
import threading
import time
from datetime import datetime
import sys
import json
import os
import select


class ChatClientFixed:
    """
    在线聊天程序客户端类 - 修复版
    修复消息接收和显示问题
    """
    
    def __init__(self):
        """初始化客户端"""
        self.socket = None
        self.connected = False
        self.username = ""
        self.server_host = "localhost"
        self.server_port = 8888
        self.receive_thread = None
        self.keep_running = True
        
        # 创建主窗口
        self.root = tk.Tk()
        self.root.title("在线聊天程序 v1.0 (修复版)")
        self.root.geometry("750x600")
        self.root.resizable(True, True)
        
        # 设置窗口图标
        try:
            self.root.iconbitmap(default="chat_icon.ico")
        except:
            pass
        
        # 设置关闭窗口事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 初始化界面
        self.setup_ui()
        
        # 加载配置
        self.load_config()
        
        # 调试模式
        self.debug_mode = True
    
    def setup_ui(self):
        """设置用户界面"""
        # 设置样式
        self.setup_styles()
        
        # 连接框架
        conn_frame = ttk.LabelFrame(self.root, text="连接设置", padding=10)
        conn_frame.pack(padx=10, pady=5, fill=tk.X)
        
        # 服务器设置
        ttk.Label(conn_frame, text="服务器地址:").grid(row=0, column=0, padx=5, sticky="w")
        self.host_entry = ttk.Entry(conn_frame, width=20)
        self.host_entry.insert(0, self.server_host)
        self.host_entry.grid(row=0, column=1, padx=5, sticky="w")
        
        ttk.Label(conn_frame, text="端口:").grid(row=0, column=2, padx=5, sticky="w")
        self.port_entry = ttk.Entry(conn_frame, width=8)
        self.port_entry.insert(0, str(self.server_port))
        self.port_entry.grid(row=0, column=3, padx=5, sticky="w")
        
        ttk.Label(conn_frame, text="用户名:").grid(row=0, column=4, padx=5, sticky="w")
        self.user_entry = ttk.Entry(conn_frame, width=15)
        self.user_entry.grid(row=0, column=5, padx=5, sticky="w")
        
        self.connect_btn = ttk.Button(
            conn_frame, 
            text="连接服务器", 
            command=self.toggle_connection,
            width=12
        )
        self.connect_btn.grid(row=0, column=6, padx=5)
        
        # 调试按钮
        self.debug_btn = ttk.Button(
            conn_frame,
            text="调试模式",
            command=self.toggle_debug,
            width=8
        )
        self.debug_btn.grid(row=0, column=7, padx=5)
        
        # 主内容框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)
        
        # 用户列表
        user_frame = ttk.LabelFrame(main_frame, text="在线用户", padding=5)
        user_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=5)
        
        # 用户列表容器
        user_list_container = ttk.Frame(user_frame)
        user_list_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 用户列表框和滚动条
        user_list_scrollbar = ttk.Scrollbar(user_list_container)
        user_list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.user_listbox = tk.Listbox(
            user_list_container, 
            width=20, 
            height=25,
            yscrollcommand=user_list_scrollbar.set
        )
        self.user_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        user_list_scrollbar.config(command=self.user_listbox.yview)
        
        self.user_listbox.bind('<<ListboxSelect>>', self.on_user_select)
        
        # 聊天区域
        chat_frame = ttk.Frame(main_frame)
        chat_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # 消息显示区域
        msg_frame = ttk.LabelFrame(chat_frame, text="聊天记录", padding=5)
        msg_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.chat_display = scrolledtext.ScrolledText(
            msg_frame, 
            width=50, 
            height=25, 
            state=tk.DISABLED,
            wrap=tk.WORD,
            font=("Microsoft YaHei", 10)
        )
        self.chat_display.pack(fill=tk.BOTH, expand=True)
        
        # 消息发送区域
        send_frame = ttk.LabelFrame(chat_frame, text="发送消息", padding=5)
        send_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(send_frame, text="消息:").pack(side=tk.LEFT)
        
        self.message_entry = ttk.Entry(send_frame)
        self.message_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        self.message_entry.bind('<Return>', self.send_message)
        
        self.send_btn = ttk.Button(
            send_frame, 
            text="发送", 
            command=self.send_message, 
            state=tk.DISABLED,
            width=8
        )
        self.send_btn.pack(side=tk.RIGHT)
        
        # 状态栏
        status_frame = ttk.Frame(self.root)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.status_var = tk.StringVar()
        self.status_var.set("未连接到服务器")
        status_bar = ttk.Label(
            status_frame, 
            textvariable=self.status_var, 
            relief=tk.SUNKEN,
            padding=2
        )
        status_bar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # 在线人数显示
        self.user_count_var = tk.StringVar()
        self.user_count_var.set("在线: 0")
        user_count_label = ttk.Label(
            status_frame, 
            textvariable=self.user_count_var,
            relief=tk.SUNKEN,
            padding=2,
            width=10
        )
        user_count_label.pack(side=tk.RIGHT)
        
        # 调试信息显示
        self.debug_var = tk.StringVar()
        self.debug_var.set("调试: 关闭")
        debug_label = ttk.Label(
            status_frame,
            textvariable=self.debug_var,
            relief=tk.SUNKEN,
            padding=2,
            width=12
        )
        debug_label.pack(side=tk.RIGHT)
    
    def setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        
        try:
            style.theme_use('clam')
        except:
            pass
        
        style.configure("TLabel", font=("Microsoft YaHei", 9))
        style.configure("TButton", font=("Microsoft YaHei", 9))
        style.configure("TEntry", font=("Microsoft YaHei", 9))
    
    def toggle_debug(self):
        """切换调试模式"""
        self.debug_mode = not self.debug_mode
        if self.debug_mode:
            self.debug_var.set("调试: 开启")
            self.debug_btn.config(text="关闭调试")
            self.display_message("系统", "调试模式已开启", "system")
        else:
            self.debug_var.set("调试: 关闭")
            self.debug_btn.config(text="调试模式")
            self.display_message("系统", "调试模式已关闭", "system")
    
    def on_user_select(self, event):
        """选择用户进行私聊"""
        if not self.connected:
            return
            
        selection = self.user_listbox.curselection()
        if selection:
            selected_user = self.user_listbox.get(selection[0])
            if selected_user != self.username and selected_user != "所有人":
                current_text = self.message_entry.get()
                if not current_text.startswith(f"@{selected_user} "):
                    self.message_entry.delete(0, tk.END)
                    self.message_entry.insert(0, f"@{selected_user} ")
                self.message_entry.focus()
    
    def toggle_connection(self):
        """连接/断开连接"""
        if not self.connected:
            self.connect_to_server()
        else:
            self.disconnect_from_server()
    
    def connect_to_server(self):
        """连接到服务器"""
        try:
            self.server_host = self.host_entry.get().strip()
            self.server_port = int(self.port_entry.get())
            self.username = self.user_entry.get().strip()
            
            if not self.username:
                messagebox.showerror("错误", "请输入用户名")
                return
                
            if not self.server_host:
                messagebox.showerror("错误", "请输入服务器地址")
                return
            
            # 更新状态
            self.status_var.set(f"正在连接 {self.server_host}:{self.server_port}...")
            self.root.update()
            
            # 创建socket连接
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(10)  # 设置连接超时10秒
            
            # 尝试连接
            if self.debug_mode:
                print(f"[调试] 尝试连接到 {self.server_host}:{self.server_port}")
            
            self.socket.connect((self.server_host, self.server_port))
            self.socket.settimeout(0.5)  # 设置非阻塞接收
            self.connected = True
            
            # 发送登录消息
            timestamp = str(int(time.time()))
            login_msg = f"LOGIN|{self.username}|SERVER|登录|{timestamp}\n"
            
            if self.debug_mode:
                print(f"[调试] 发送登录消息: {login_msg.strip()}")
            
            self.socket.send(login_msg.encode('utf-8'))
            
            # 更新UI
            self.connect_btn.config(text="断开连接")
            self.send_btn.config(state=tk.NORMAL)
            self.status_var.set(f"已连接到 {self.server_host}:{self.server_port}")
            
            # 禁用连接设置输入框
            self.host_entry.config(state=tk.DISABLED)
            self.port_entry.config(state=tk.DISABLED)
            self.user_entry.config(state=tk.DISABLED)
            
            # 启动接收消息线程
            self.keep_running = True
            self.receive_thread = threading.Thread(target=self.receive_messages, daemon=True)
            self.receive_thread.start()
            
            self.display_message("系统", f"成功连接到聊天服务器，欢迎 {self.username}！", "system")
            
            # 保存配置
            self.save_config()
            
        except socket.timeout:
            messagebox.showerror("连接错误", 
                f"连接服务器超时（10秒）\n"
                f"请检查：\n"
                f"1. 服务器地址是否正确\n"
                f"2. 服务器是否正在运行\n"
                f"3. 防火墙设置")
            self.status_var.set("连接超时")
        except ConnectionRefusedError:
            messagebox.showerror("连接错误",
                f"连接被拒绝\n"
                f"可能的原因：\n"
                f"1. 服务器未启动\n"
                f"2. 端口号错误\n"
                f"3. 服务器防火墙阻止了连接")
            self.status_var.set("连接被拒绝")
        except socket.gaierror:
            messagebox.showerror("连接错误", "无法解析服务器地址，请检查地址是否正确")
            self.status_var.set("地址解析失败")
        except Exception as e:
            messagebox.showerror("连接错误", 
                f"连接失败: {str(e)}\n"
                f"请检查网络连接和服务器状态")
            self.status_var.set("连接失败")
    
    def disconnect_from_server(self):
        """断开服务器连接"""
        self.keep_running = False
        
        if self.connected and self.socket:
            try:
                # 发送退出消息
                timestamp = str(int(time.time()))
                logout_msg = f"LOGOUT|{self.username}|SERVER|退出|{timestamp}\n"
                
                if self.debug_mode:
                    print(f"[调试] 发送退出消息: {logout_msg.strip()}")
                
                self.socket.send(logout_msg.encode('utf-8'))
                time.sleep(0.5)  # 等待消息发送
            except:
                pass
            
            self.connected = False
            
            try:
                self.socket.close()
            except:
                pass
            
            # 更新UI
            self.connect_btn.config(text="连接服务器")
            self.send_btn.config(state=tk.DISABLED)
            self.status_var.set("未连接到服务器")
            self.user_listbox.delete(0, tk.END)
            self.user_count_var.set("在线: 0")
            
            # 启用连接设置输入框
            self.host_entry.config(state=tk.NORMAL)
            self.port_entry.config(state=tk.NORMAL)
            self.user_entry.config(state=tk.NORMAL)
            
            self.display_message("系统", "已断开与服务器的连接", "system")
    
    def send_message(self, event=None):
        """发送消息"""
        if not self.connected:
            messagebox.showerror("错误", "未连接到服务器")
            return
        
        message = self.message_entry.get().strip()
        if not message:
            return
        
        # 解析接收者
        receiver = "ALL"
        original_message = message  # 保存原始消息
        
        if message.startswith('@'):
            parts = message.split(' ', 1)
            if len(parts) > 1:
                receiver = parts[0][1:]  # 移除@符号
                message = parts[1]
        
        # 构建消息
        timestamp = str(int(time.time()))
        msg = f"MESSAGE|{self.username}|{receiver}|{message}|{timestamp}\n"
        
        if self.debug_mode:
            print(f"[调试] 发送消息: {msg.strip()}")
        
        try:
            self.socket.send(msg.encode('utf-8'))
            self.message_entry.delete(0, tk.END)
            
            # 显示自己发送的消息
            if receiver == "ALL":
                self.display_message("我", message, "self")
            else:
                self.display_message(f"我对 {receiver}", message, "private")
                
        except Exception as e:
            if self.debug_mode:
                print(f"[错误] 发送消息失败: {e}")
            messagebox.showerror("发送错误", f"发送消息失败: {str(e)}")
            self.disconnect_from_server()
    
    def receive_messages(self):
        """接收服务器消息 - 修复版"""
        buffer = ""
        if self.debug_mode:
            print("[调试] 启动消息接收线程")
        
        while self.connected and self.keep_running:
            try:
                # 使用select检测是否有数据可读
                ready_to_read, _, _ = select.select([self.socket], [], [], 0.1)
                
                if ready_to_read:
                    # 接收数据
                    data = self.socket.recv(4096).decode('utf-8', errors='ignore')
                    
                    if data:
                        buffer += data
                        
                        if self.debug_mode:
                            print(f"[调试] 收到数据: {repr(data)}")
                            print(f"[调试] 当前缓冲区: {repr(buffer)}")
                        
                        # 按换行符分割完整消息
                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            line = line.strip()
                            
                            if line:
                                if self.debug_mode:
                                    print(f"[调试] 处理完整消息: {line}")
                                
                                # 在主线程中处理消息
                                self.root.after(0, lambda l=line: self.process_message(l))
                    
                    elif len(data) == 0:
                        # 连接关闭
                        if self.debug_mode:
                            print("[调试] 服务器断开连接")
                        break
                
            except socket.timeout:
                # 超时是正常的，继续循环
                continue
            except socket.error as e:
                if self.connected:
                    if self.debug_mode:
                        print(f"[错误] Socket错误: {e}")
                break
            except Exception as e:
                if self.connected:
                    if self.debug_mode:
                        print(f"[错误] 接收消息错误: {e}")
                break
        
        # 连接断开
        if self.connected:
            if self.debug_mode:
                print("[调试] 接收线程结束，触发断开连接")
            self.root.after(0, self.disconnect_from_server)
    
    def process_message(self, message):
        """处理接收到的消息"""
        if self.debug_mode:
            print(f"[调试] 处理消息: {message}")
        
        # 解析消息
        parts = message.split('|')
        if len(parts) >= 5:
            msg_type, sender, receiver, content, timestamp = parts
            
            if self.debug_mode:
                print(f"[调试] 解析结果: 类型={msg_type}, 发送者={sender}, 接收者={receiver}, 内容={content}")
            
            if msg_type == "MESSAGE" or msg_type == "SYSTEM":
                # 显示消息
                if sender == "SERVER":
                    self.display_message("系统", content, "system")
                else:
                    if receiver == "ALL":
                        self.display_message(sender, content, "other")
                    else:
                        # 私聊消息
                        if sender == self.username:
                            self.display_message(f"我对 {receiver}", content, "private")
                        else:
                            self.display_message(f"{sender} 对我说", content, "private")
            
            elif msg_type == "USERLIST":
                # 更新用户列表
                if self.debug_mode:
                    print(f"[调试] 更新用户列表: {content}")
                self.update_user_list(content)
            else:
                if self.debug_mode:
                    print(f"[调试] 未知消息类型: {msg_type}")
        else:
            if self.debug_mode:
                print(f"[调试] 消息格式错误，部分数: {len(parts)}，消息: {message}")
    
    def display_message(self, sender, content, msg_type):
        """在聊天区域显示消息"""
        self.chat_display.config(state=tk.NORMAL)
        
        # 获取当前时间
        current_time = datetime.now().strftime("%H:%M:%S")
        
        # 根据消息类型设置格式
        if msg_type == "system":
            self.chat_display.insert(tk.END, f"[{current_time}] {content}\n", "system")
            if self.debug_mode:
                print(f"[显示] 系统消息: {content}")
        elif msg_type == "self":
            self.chat_display.insert(tk.END, f"[{current_time}] {sender}: {content}\n", "self")
        elif msg_type == "private":
            self.chat_display.insert(tk.END, f"[{current_time}] {sender}: {content}\n", "private")
        else:
            self.chat_display.insert(tk.END, f"[{current_time}] {sender}: {content}\n", "other")
        
        self.chat_display.config(state=tk.DISABLED)
        self.chat_display.see(tk.END)
    
    def update_user_list(self, user_list_str):
        """更新在线用户列表"""
        self.user_listbox.delete(0, tk.END)
        self.user_listbox.insert(tk.END, "所有人")
        
        users = user_list_str.split(',')
        user_count = 0
        
        for user in users:
            if user and user != self.username:
                self.user_listbox.insert(tk.END, user)
                user_count += 1
        
        # 更新在线人数显示（不包括自己）
        self.user_count_var.set(f"在线: {user_count}")
        
        if self.debug_mode:
            print(f"[调试] 用户列表更新: {users}, 在线人数: {user_count}")
    
    def save_config(self):
        """保存连接配置"""
        config = {
            "server_host": self.server_host,
            "server_port": self.server_port,
            "username": self.username
        }
        
        try:
            with open("chat_config_fixed.json", "w", encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            if self.debug_mode:
                print(f"[调试] 配置保存成功: {config}")
        except Exception as e:
            print(f"保存配置失败: {e}")
    
    def load_config(self):
        """加载连接配置"""
        try:
            config_file = "chat_config_fixed.json"
            if os.path.exists(config_file):
                with open(config_file, "r", encoding='utf-8') as f:
                    config = json.load(f)
                
                self.server_host = config.get("server_host", "localhost")
                self.server_port = config.get("server_port", 8888)
                self.username = config.get("username", "")
                
                self.host_entry.delete(0, tk.END)
                self.host_entry.insert(0, self.server_host)
                
                self.port_entry.delete(0, tk.END)
                self.port_entry.insert(0, str(self.server_port))
                
                self.user_entry.delete(0, tk.END)
                self.user_entry.insert(0, self.username)
                
                if self.debug_mode:
                    print(f"[调试] 配置加载成功: {config}")
        except Exception as e:
            print(f"加载配置失败: {e}")
    
    def on_closing(self):
        """关闭窗口时的处理"""
        self.keep_running = False
        if self.connected:
            self.disconnect_from_server()
        self.root.destroy()
    
    def run(self):
        """运行客户端"""
        # 配置文本样式
        self.chat_display.tag_config("system", foreground="blue", font=("Microsoft YaHei", 9, "italic"))
        self.chat_display.tag_config("self", foreground="green", font=("Microsoft YaHei", 10))
        self.chat_display.tag_config("other", foreground="black", font=("Microsoft YaHei", 10))
        self.chat_display.tag_config("private", foreground="purple", font=("Microsoft YaHei", 10, "bold"))
        
        # 居中显示窗口
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        
        # 聚焦到用户名输入框
        self.user_entry.focus()
        
        # 启动主循环
        self.root.mainloop()


def main():
    """主函数"""
    print("=" * 60)
    print("在线聊天程序客户端 (修复版)")
    print("版本: 1.1 (修复消息不可见问题)")
    print("作者: 网络通信编程课程项目")
    print("=" * 60)
    print("提示: 请确保服务器端已启动 (chat_server_fixed)")
    print("=" * 60)
    
    try:
        # 创建并运行客户端
        client = ChatClientFixed()
        client.run()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")
        messagebox.showerror("错误", f"程序启动失败: {e}")
    finally:
        print("聊天客户端已退出")


if __name__ == "__main__":
    main()