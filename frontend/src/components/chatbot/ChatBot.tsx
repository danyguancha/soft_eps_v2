// src/components/chatbot/ChatBot.tsx - CON EMOJI PICKER FUNCIONAL
import React, { useState, useCallback, useEffect, useRef } from 'react';
import { Input, Button, message, Avatar } from 'antd';
import { SendOutlined, RobotOutlined, SmileOutlined } from '@ant-design/icons';
import EmojiPicker, { type EmojiClickData, EmojiStyle, SkinTones } from 'emoji-picker-react';
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
  const [showEmojiPicker, setShowEmojiPicker] = useState(false); // ✅ Estado para emoji picker
  const [cursorPosition, setCursorPosition] = useState(0); // ✅ Posición del cursor
  
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<any>(null); // ✅ Ref para el textarea
  const emojiPickerRef = useRef<HTMLDivElement>(null); // ✅ Ref para el emoji picker

  // Auto-scroll al último mensaje
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Mensaje de bienvenida inicial
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

  // ✅ CERRAR EMOJI PICKER AL HACER CLICK FUERA
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

  // ✅ GUARDAR POSICIÓN DEL CURSOR
  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInputValue(e.target.value);
    setCursorPosition(e.target.selectionStart || 0);
  }, []);

  // ✅ ACTUALIZAR POSICIÓN DEL CURSOR AL HACER CLICK O MOVER
  const handleInputClick = useCallback((e: React.MouseEvent<HTMLTextAreaElement>) => {
    const target = e.target as HTMLTextAreaElement;
    setCursorPosition(target.selectionStart || 0);
  }, []);

  // ✅ MANEJAR SELECCIÓN DE EMOJI
  const handleEmojiClick = useCallback((emojiData: EmojiClickData) => {
    const emoji = emojiData.emoji;
    const newValue = inputValue.slice(0, cursorPosition) + emoji + inputValue.slice(cursorPosition);
    
    setInputValue(newValue);
    setShowEmojiPicker(false);
    
    // ✅ ENFOCAR EL TEXTAREA Y POSICIONAR EL CURSOR DESPUÉS DEL EMOJI
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

  // ✅ TOGGLE EMOJI PICKER
  const toggleEmojiPicker = useCallback(() => {
    setShowEmojiPicker(!showEmojiPicker);
    
    // Si se abre el picker, actualizar la posición del cursor
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

  // Agrupar mensajes por fecha
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
      {/* Header del Chat */}
      <div className="whatsapp-header">
        <div className="whatsapp-header-info">
          <Avatar 
            size={40} 
            icon={<RobotOutlined />} 
            className="whatsapp-avatar-header"
          />
          <div className="whatsapp-header-text">
            <div className="whatsapp-contact-name">Asistente IA</div>
            <div className="whatsapp-status">
              {loading ? 'escribiendo...' : 'en línea'}
            </div>
          </div>
        </div>
      </div>

      {/* Área de Mensajes */}
      <div className="whatsapp-messages-area">
        {Object.entries(groupedMessages).map(([dateKey, dayMessages]) => (
          <div key={dateKey}>
            <div className="whatsapp-date-separator">
              <span className="whatsapp-date-text">
                {formatDate(new Date(dateKey))}
              </span>
            </div>

            {dayMessages.map((message) => (
              <div
                key={message.id}
                className={`whatsapp-message ${
                  message.type === 'user' ? 'whatsapp-message-sent' : 'whatsapp-message-received'
                }`}
              >
                {message.type === 'ai' && (
                  <Avatar 
                    size={32} 
                    icon={<RobotOutlined />} 
                    className="whatsapp-message-avatar"
                  />
                )}
                
                <div className={`whatsapp-bubble ${
                  message.type === 'user' ? 'whatsapp-bubble-sent' : 'whatsapp-bubble-received'
                }`}>
                  <div className="whatsapp-message-content">
                    {message.content}
                  </div>
                  <div className="whatsapp-message-time">
                    {formatTime(message.timestamp)}
                    {message.type === 'user' && (
                      <span className={`whatsapp-status-icon ${
                        message.status === 'sending' ? 'sending' : 
                        message.status === 'error' ? 'error' : 'sent'
                      }`}>
                        {message.status === 'sending' ? '🕒' : 
                         message.status === 'error' ? '❗' : '✓'}
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

      {/* Área de Input */}
      <div className="whatsapp-input-area">
        {/* ✅ EMOJI PICKER */}
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
