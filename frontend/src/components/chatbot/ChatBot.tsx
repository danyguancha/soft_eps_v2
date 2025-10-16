// src/components/chatbot/ChatBot.tsx
import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Input, Button, message, Avatar } from 'antd';
import { SendOutlined, RobotOutlined, SmileOutlined } from '@ant-design/icons';
import EmojiPicker, { type EmojiClickData, EmojiStyle, SkinTones } from 'emoji-picker-react';
import ReactMarkdown from 'react-markdown';
import { AIService } from '../../services/AIService';
import './ChatBot.css';

interface ChatMessage {
  id: string;
  type: 'user' | 'ai';
  content: string;
  timestamp: Date;
  status?: 'sending' | 'sent' | 'error';
}

interface ChatBotProps {
  fileContext?: string;
}

export const ChatBot: React.FC<ChatBotProps> = ({ fileContext }) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [cursorPosition, setCursorPosition] = useState(0);
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<any>(null);
  const emojiPickerRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    const welcomeMessage: ChatMessage = {
      id: 'welcome',
      type: 'ai',
      content: '¡Hola! 👋 Soy tu asistente para análisis de datos. ¿En qué puedo ayudarte hoy?',
      timestamp: new Date(),
      status: 'sent'
    };
    setMessages([welcomeMessage]);
  }, []);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (emojiPickerRef.current && !emojiPickerRef.current.contains(event.target as Node)) {
        setShowEmojiPicker(false);
      }
    };

    if (showEmojiPicker) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [showEmojiPicker]);

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInputValue(e.target.value);
    setCursorPosition(e.target.selectionStart || 0);
  }, []);

  const handleInputClick = useCallback((e: React.MouseEvent<HTMLTextAreaElement>) => {
    const target = e.target as HTMLTextAreaElement;
    setCursorPosition(target.selectionStart || 0);
  }, []);

  const handleEmojiClick = useCallback((emojiData: EmojiClickData) => {
    const emoji = emojiData.emoji;
    const newValue = inputValue.slice(0, cursorPosition) + emoji + inputValue.slice(cursorPosition);
    
    setInputValue(newValue);
    setShowEmojiPicker(false);
    
    setTimeout(() => {
      if (textareaRef.current) {
        const textarea = textareaRef.current.resizableTextArea?.textArea;
        if (textarea) {
          textarea.focus();
          const newCursorPosition = cursorPosition + emoji.length;
          textarea.setSelectionRange(newCursorPosition, newCursorPosition);
          setCursorPosition(newCursorPosition);
        }
      }
    }, 0);
  }, [inputValue, cursorPosition]);

  const toggleEmojiPicker = useCallback(() => {
    setShowEmojiPicker(!showEmojiPicker);
    
    if (!showEmojiPicker && textareaRef.current) {
      const textarea = textareaRef.current.resizableTextArea?.textArea;
      if (textarea) {
        setCursorPosition(textarea.selectionStart || inputValue.length);
      }
    }
  }, [showEmojiPicker, inputValue.length]);

  const sendMessage = useCallback(async () => {
    if (!inputValue.trim()) return;

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      type: 'user',
      content: inputValue,
      timestamp: new Date(),
      status: 'sending'
    };

    setMessages(prev => [...prev, userMessage]);
    setLoading(true);
    setInputValue('');
    setCursorPosition(0);

    try {
      setMessages(prev => prev.map(msg => 
        msg.id === userMessage.id ? { ...msg, status: 'sent' } : msg
      ));

      const response = await AIService.askAI({
        question: inputValue,
        file_context: fileContext,
      });

      const aiMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        type: 'ai',
        content: response.response,
        timestamp: new Date(),
        status: 'sent'
      };

      setMessages(prev => [...prev, aiMessage]);
    } catch (error) {
      message.error('Error al comunicarse con la IA');
      setMessages(prev => prev.map(msg => 
        msg.id === userMessage.id ? { ...msg, status: 'error' } : msg
      ));
    } finally {
      setLoading(false);
    }
  }, [inputValue, fileContext]);

  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }, [sendMessage]);

  const formatTime = (timestamp: Date) => {
    return timestamp.toLocaleTimeString('es-ES', { 
      hour: '2-digit', 
      minute: '2-digit' 
    });
  };

  const formatDate = (timestamp: Date) => {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    
    if (timestamp.toDateString() === today.toDateString()) {
      return 'Hoy';
    } else if (timestamp.toDateString() === yesterday.toDateString()) {
      return 'Ayer';
    } else {
      return timestamp.toLocaleDateString('es-ES');
    }
  };

  const groupedMessages = messages.reduce((groups, message) => {
    const dateKey = message.timestamp.toDateString();
    if (!groups[dateKey]) {
      groups[dateKey] = [];
    }
    groups[dateKey].push(message);
    return groups;
  }, {} as Record<string, ChatMessage[]>);

  return (
    <div className="whatsapp-chat-container">
      <div className="whatsapp-header">
        <div className="whatsapp-header-info">
          <Avatar 
            size={40} 
            icon={<RobotOutlined />} 
            className="whatsapp-avatar-header"
          />
          <div className="whatsapp-header-text">
            <div className="whatsapp-contact-name">Asistente IA</div>
            <div className={`whatsapp-status ${loading ? 'typing' : 'online'}`}>
              {loading ? 'escribiendo...' : 'en línea'}
            </div>
          </div>
        </div>
      </div>

      <div className="whatsapp-messages-area">
        {Object.entries(groupedMessages).map(([dateKey, dayMessages]) => (
          <div key={dateKey}>
            <div className="whatsapp-date-separator">
              <span className="whatsapp-date-text">
                {formatDate(new Date(dateKey))}
              </span>
            </div>

            {dayMessages.map((msg) => (
              <div
                key={msg.id}
                className={`whatsapp-message ${
                  msg.type === 'user' ? 'whatsapp-message-sent' : 'whatsapp-message-received'
                }`}
              >
                {msg.type === 'ai' && (
                  <Avatar 
                    size={32} 
                    icon={<RobotOutlined />} 
                    className="whatsapp-message-avatar"
                  />
                )}
                
                <div className={`whatsapp-bubble ${
                  msg.type === 'user' ? 'whatsapp-bubble-sent' : 'whatsapp-bubble-received'
                }`}>
                  <div className="whatsapp-message-content">
                    {msg.type === 'ai' ? (
                      <ReactMarkdown
                        components={{
                          p: ({node, ...props}) => <p style={{margin: '0.2em 0', lineHeight: '1.4'}} {...props} />,
                          h1: ({node, ...props}) => <h3 style={{margin: '0.3em 0 0.2em', fontSize: '1.1em', fontWeight: 600, color: 'inherit'}} {...props} />,
                          h2: ({node, ...props}) => <h4 style={{margin: '0.3em 0 0.2em', fontSize: '1.05em', fontWeight: 600, color: 'inherit'}} {...props} />,
                          h3: ({node, ...props}) => <h5 style={{margin: '0.3em 0 0.2em', fontSize: '1em', fontWeight: 600, color: 'inherit'}} {...props} />,
                          ul: ({node, ...props}) => <ul style={{margin: '0.2em 0', paddingLeft: '1.2em'}} {...props} />,
                          ol: ({node, ...props}) => <ol style={{margin: '0.2em 0', paddingLeft: '1.2em'}} {...props} />,
                          li: ({node, ...props}) => <li style={{marginBottom: '0.1em', lineHeight: '1.4'}} {...props} />,
                          code: ({node, inline, ...props}: any) => 
                            inline ? (
                              <code style={{
                                backgroundColor: 'var(--whatsapp-overlay-bg)',
                                padding: '2px 5px',
                                borderRadius: '3px',
                                fontSize: '0.9em'
                              }} {...props} />
                            ) : (
                              <code style={{
                                display: 'block',
                                backgroundColor: 'var(--whatsapp-overlay-bg)',
                                padding: '8px',
                                borderRadius: '6px',
                                overflowX: 'auto',
                                fontSize: '0.85em',
                                margin: '0.3em 0'
                              }} {...props} />
                            ),
                          pre: ({node, ...props}) => <pre style={{margin: 0}} {...props} />,
                          blockquote: ({node, ...props}) => (
                            <blockquote style={{
                              borderLeft: '3px solid var(--whatsapp-accent-color)',
                              paddingLeft: '0.8em',
                              margin: '0.3em 0',
                              fontStyle: 'italic',
                              opacity: 0.85
                            }} {...props} />
                          ),
                          a: ({node, ...props}) => (
                            <a style={{
                              color: 'var(--whatsapp-accent-color)',
                              textDecoration: 'underline'
                            }} target="_blank" rel="noopener noreferrer" {...props} />
                          ),
                          strong: ({node, ...props}) => <strong style={{fontWeight: 600}} {...props} />,
                          em: ({node, ...props}) => <em style={{fontStyle: 'italic'}} {...props} />,
                          hr: ({node, ...props}) => (
                            <hr style={{
                              margin: '0.5em 0',
                              border: 'none',
                              borderTop: '1px solid var(--whatsapp-border-color)',
                              opacity: 0.3
                            }} {...props} />
                          ),
                        }}
                      >
                        {msg.content}
                      </ReactMarkdown>
                    ) : (
                      msg.content
                    )}
                  </div>
                  <div className="whatsapp-message-time">
                    {formatTime(msg.timestamp)}
                    {msg.type === 'user' && (
                      <span className={`whatsapp-status-icon ${
                        msg.status === 'sending' ? 'sending' : 
                        msg.status === 'error' ? 'error' : 'sent'
                      }`}>
                        {msg.status === 'sending' ? '🕒' : 
                         msg.status === 'error' ? '❗' : '✓'}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        ))}

        {loading && (
          <div className="whatsapp-message whatsapp-message-received">
            <Avatar 
              size={32} 
              icon={<RobotOutlined />} 
              className="whatsapp-message-avatar"
            />
            <div className="whatsapp-bubble whatsapp-bubble-received whatsapp-typing">
              <div className="whatsapp-typing-indicator">
                <span></span>
                <span></span>
                <span></span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <div className="whatsapp-input-area">
        {showEmojiPicker && (
          <div className="whatsapp-emoji-picker-container" ref={emojiPickerRef}>
            <EmojiPicker
              onEmojiClick={handleEmojiClick}
              autoFocusSearch={false}
              emojiStyle={EmojiStyle.NATIVE}
              defaultSkinTone={SkinTones.MEDIUM}
              searchDisabled={false}
              skinTonesDisabled={false}
              width={320}
              height={400}
              previewConfig={{
                defaultEmoji: "1f60a",
                defaultCaption: "¿Qué emoji buscas?",
                showPreview: true
              }}
            />
          </div>
        )}

        <div className="whatsapp-input-container">
          <Button
            type="text"
            icon={<SmileOutlined />}
            className={`whatsapp-emoji-button ${showEmojiPicker ? 'active' : ''}`}
            disabled={loading}
            onClick={toggleEmojiPicker}
          />
          
          <Input.TextArea
            ref={textareaRef}
            placeholder="Escribe un mensaje..."
            value={inputValue}
            onChange={handleInputChange}
            onClick={handleInputClick}
            onKeyDown={handleKeyPress}
            disabled={loading}
            className="whatsapp-input"
            autoSize={{ minRows: 1, maxRows: 4 }}
            bordered={false}
          />
          
          <Button
            type="text"
            icon={<SendOutlined />}
            onClick={sendMessage}
            disabled={loading || !inputValue.trim()}
            className={`whatsapp-send-button ${
              inputValue.trim() ? 'whatsapp-send-active' : ''
            }`}
          />
        </div>
      </div>
    </div>
  );
};
