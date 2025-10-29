// src/components/chatbot/ChatBot.tsx
import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Input, Button, message, Avatar, Badge, Tooltip } from 'antd';
import { SendOutlined, RobotOutlined, SmileOutlined, MessageOutlined, CloseOutlined } from '@ant-design/icons';
import EmojiPicker, { type EmojiClickData, EmojiStyle, SkinTones } from 'emoji-picker-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
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
  // Estados del chat
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [cursorPosition, setCursorPosition] = useState(0);
  
  // EvalNoteBotEstados para flotante
  const [isOpen, setIsOpen] = useState(false);
  const [unreadCount, setUnreadCount] = useState(0);
  const [isAnimating, setIsAnimating] = useState(false);
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<any>(null);
  const emojiPickerRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Mensaje de bienvenida
  useEffect(() => {
    const welcomeMessage: ChatMessage = {
      id: 'welcome',
      type: 'ai',
      content: '¡Hola! 👋 Soy **EvalNoteBot**, tu asistente para análisis de datos.\n\n¿En qué puedo ayudarte hoy?',
      timestamp: new Date(),
      status: 'sent'
    };
    setMessages([welcomeMessage]);
  }, []);

  // Animación inicial del botón
  useEffect(() => {
    setTimeout(() => {
      setIsAnimating(true);
      setTimeout(() => setIsAnimating(false), 500);
    }, 1000);
  }, []);

  // Click fuera del emoji picker
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

  // EvalNoteBotToggle chat flotante
  const handleToggleChat = () => {
    setIsOpen(!isOpen);
    if (!isOpen) {
      setUnreadCount(0); // Limpiar contador al abrir
    }
  };

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
      
      // EvalNoteBotIncrementar contador si está cerrado
      if (!isOpen) {
        setUnreadCount(prev => prev + 1);
      }
      
    } catch (error) {
      console.error('Error al comunicarse con la IA:', error);
      message.error('Error al comunicarse con la IA');
      
      setMessages(prev => prev.map(msg => 
        msg.id === userMessage.id ? { ...msg, status: 'error' } : msg
      ));
      
      const errorMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        type: 'ai',
        content: 'Lo siento, ocurrió un error al procesar tu consulta. Por favor, intenta de nuevo.',
        timestamp: new Date(),
        status: 'sent'
      };
      
      setMessages(prev => [...prev, errorMessage]);
      
      if (!isOpen) {
        setUnreadCount(prev => prev + 1);
      }
    } finally {
      setLoading(false);
    }
  }, [inputValue, fileContext, isOpen]);

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
    <>
      {/* EvalNoteBotVENTANA DEL CHAT FLOTANTE */}
      <div className={`floating-chat-window ${isOpen ? 'open' : 'closed'}`}>
        <div className="whatsapp-chat-container">
          {/* Header con botón de cerrar */}
          <div className="whatsapp-header">
            <div className="whatsapp-header-info">
              <Avatar 
                size={40} 
                icon={<RobotOutlined />} 
                className="whatsapp-avatar-header"
              />
              <div className="whatsapp-header-text">
                <div className="whatsapp-contact-name">EvalNoteBot - Asistente IA</div>
                <div className={`whatsapp-status ${loading ? 'typing' : 'online'}`}>
                  {loading ? 'escribiendo...' : 'en línea'}
                </div>
              </div>
            </div>
            
            {/* EvalNoteBotBotón de cerrar */}
            <button 
              className="floating-chat-close"
              onClick={handleToggleChat}
              aria-label="Cerrar chat"
            >
              <CloseOutlined />
            </button>
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
                            remarkPlugins={[remarkGfm]}
                            components={{
                              p: ({node, ...props}) => (
                                <p style={{ margin: '0.4em 0', lineHeight: '1.5', color: 'inherit' }} {...props} />
                              ),
                              h1: ({node, ...props}) => (
                                <h1 style={{ margin: '0.8em 0 0.4em', fontSize: '1.3em', fontWeight: 700, color: 'inherit', borderBottom: '2px solid rgba(255,255,255,0.3)', paddingBottom: '0.3em' }} {...props} />
                              ),
                              h2: ({node, ...props}) => (
                                <h2 style={{ margin: '0.7em 0 0.3em', fontSize: '1.2em', fontWeight: 700, color: 'inherit' }} {...props} />
                              ),
                              h3: ({node, ...props}) => (
                                <h3 style={{ margin: '0.6em 0 0.3em', fontSize: '1.1em', fontWeight: 600, color: 'inherit' }} {...props} />
                              ),
                              ul: ({node, ...props}) => (
                                <ul style={{ margin: '0.5em 0', paddingLeft: '1.5em', listStyleType: 'disc' }} {...props} />
                              ),
                              ol: ({node, ...props}) => (
                                <ol style={{ margin: '0.5em 0', paddingLeft: '1.5em' }} {...props} />
                              ),
                              li: ({node, ...props}) => (
                                <li style={{ marginBottom: '0.3em', lineHeight: '1.5' }} {...props} />
                              ),
                              strong: ({node, ...props}) => (
                                <strong style={{ fontWeight: 700, background: 'rgba(255, 255, 255, 0.15)', padding: '2px 5px', borderRadius: '3px' }} {...props} />
                              ),
                              em: ({node, ...props}) => (
                                <em style={{ fontStyle: 'italic', opacity: 0.9 }} {...props} />
                              ),
                              code: ({node, inline, ...props}: any) => 
                                inline ? (
                                  <code style={{ backgroundColor: 'rgba(0, 0, 0, 0.2)', padding: '2px 6px', borderRadius: '4px', fontSize: '0.9em', fontFamily: 'monospace' }} {...props} />
                                ) : (
                                  <code style={{ display: 'block', backgroundColor: 'rgba(0, 0, 0, 0.3)', padding: '10px', borderRadius: '6px', overflowX: 'auto', fontSize: '0.85em', margin: '0.5em 0', fontFamily: 'monospace' }} {...props} />
                                ),
                              pre: ({node, ...props}) => (
                                <pre style={{ margin: '0.5em 0', overflow: 'auto' }} {...props} />
                              ),
                              blockquote: ({node, ...props}) => (
                                <blockquote style={{ borderLeft: '4px solid rgba(255, 255, 255, 0.5)', paddingLeft: '1em', margin: '0.5em 0', fontStyle: 'italic', opacity: 0.9 }} {...props} />
                              ),
                              a: ({node, ...props}) => (
                                <a style={{ color: '#53bdeb', textDecoration: 'underline', fontWeight: 500 }} target="_blank" rel="noopener noreferrer" {...props} />
                              ),
                              hr: ({node, ...props}) => (
                                <hr style={{ margin: '1em 0', border: 'none', borderTop: '1px solid rgba(255, 255, 255, 0.2)' }} {...props} />
                              ),
                              table: ({node, ...props}) => (
                                <div style={{ overflowX: 'auto', margin: '0.5em 0' }}>
                                  <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: '0.9em' }} {...props} />
                                </div>
                              ),
                              thead: ({node, ...props}) => (
                                <thead style={{ backgroundColor: 'rgba(0, 0, 0, 0.2)' }} {...props} />
                              ),
                              th: ({node, ...props}) => (
                                <th style={{ padding: '8px', textAlign: 'left', borderBottom: '2px solid rgba(255, 255, 255, 0.3)', fontWeight: 600 }} {...props} />
                              ),
                              td: ({node, ...props}) => (
                                <td style={{ padding: '8px', borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }} {...props} />
                              ),
                            }}
                          >
                            {msg.content}
                          </ReactMarkdown>
                        ) : (
                          <span style={{ whiteSpace: 'pre-wrap' }}>{msg.content}</span>
                        )}
                      </div>
                      <div className="whatsapp-message-time">
                        {formatTime(msg.timestamp)}
                        {msg.type === 'user' && (
                          <span className={`whatsapp-status-icon ${msg.status === 'sending' ? 'sending' : msg.status === 'error' ? 'error' : 'sent'}`}>
                            {msg.status === 'sending' ? '🕒' : msg.status === 'error' ? '❗' : '✓'}
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
                className={`whatsapp-send-button ${inputValue.trim() ? 'whatsapp-send-active' : ''}`}
              />
            </div>
          </div>
        </div>
      </div>

      {/* EvalNoteBotBOTÓN FLOTANTE */}
      {!isOpen && (
        <Tooltip title="Abrir chat con el asistente" placement="left">
          <Badge count={unreadCount} offset={[-5, 5]}>
            <button
              className={`floating-chat-button ${isAnimating ? 'animate-pulse' : ''}`}
              onClick={handleToggleChat}
              aria-label="Abrir chat"
            >
              <MessageOutlined className="floating-chat-button-icon" />
              <span className="floating-chat-button-pulse"></span>
            </button>
          </Badge>
        </Tooltip>
      )}
    </>
  );
};
