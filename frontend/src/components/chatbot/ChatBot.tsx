import React, { useState, useCallback } from 'react';
import { Card, Input, Button, List, Avatar, Spin, message } from 'antd';
import { SendOutlined, RobotOutlined, UserOutlined } from '@ant-design/icons';
import { AIService } from '../../services/AIService';

interface ChatMessage {
  id: string;
  type: 'user' | 'ai';
  content: string;
  timestamp: Date;
}

interface ChatBotProps {
  fileContext?: string;
}

export const ChatBot: React.FC<ChatBotProps> = ({ fileContext }) => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [loading, setLoading] = useState(false);

  const sendMessage = useCallback(async () => {
    if (!inputValue.trim()) return;

    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      type: 'user',
      content: inputValue,
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setLoading(true);

    try {
      const response = await AIService.askAI({
        question: inputValue,
        file_context: fileContext,
      });

      const aiMessage: ChatMessage = {
        id: (Date.now() + 1).toString(),
        type: 'ai',
        content: response.response,
        timestamp: new Date(),
      };

      setMessages(prev => [...prev, aiMessage]);
    } catch (error) {
      message.error('Error al comunicarse con la IA');
    } finally {
      setLoading(false);
      setInputValue('');
    }
  }, [inputValue, fileContext]);

  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }, [sendMessage]);

  return (
    <Card 
      title="Asistente IA" 
      style={{ height: 500 }}
      bodyStyle={{ padding: 0, display: 'flex', flexDirection: 'column', height: '100%' }}
    >
      {/* Lista de mensajes */}
      <div style={{ flex: 1, padding: 16, overflowY: 'auto' }}>
        <List
          dataSource={messages}
          renderItem={(message) => (
            <List.Item style={{ border: 'none', padding: '8px 0' }}>
              <List.Item.Meta
                avatar={
                  <Avatar icon={message.type === 'user' ? <UserOutlined /> : <RobotOutlined />} />
                }
                title={message.type === 'user' ? 'Tú' : 'Asistente IA'}
                description={
                  <div style={{ whiteSpace: 'pre-wrap' }}>
                    {message.content}
                  </div>
                }
              />
            </List.Item>
          )}
        />
        {loading && (
          <div style={{ textAlign: 'center', padding: 16 }}>
            <Spin /> Pensando...
          </div>
        )}
      </div>

      {/* Input de mensaje */}
      <div style={{ padding: 16, borderTop: '1px solid #f0f0f0' }}>
        <Input.Group compact>
          <Input
            style={{ width: 'calc(100% - 40px)' }}
            placeholder="Pregunta algo sobre los datos..."
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyPress={handleKeyPress}
            disabled={loading}
          />
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={sendMessage}
            disabled={loading || !inputValue.trim()}
          />
        </Input.Group>
      </div>
    </Card>
  );
};
