// src/components/tabs/tabs/WelcomeTab/WelcomeTab.tsx
import React from 'react';
import { Typography } from 'antd';
import {
    BulbOutlined,
    ThunderboltOutlined,
    StarOutlined,
    CrownOutlined,
} from '@ant-design/icons';
import type { TabProps } from '../../../types/api.types';

const { Title, Text, Paragraph } = Typography;

export const WelcomeTab: React.FC<TabProps> = ({ isMobile }) => {
    const highlights = [
        { icon: <ThunderboltOutlined className="highlight-icon lightning" />, text: "Procesamiento Ultra Rápido" },
        { icon: <StarOutlined className="highlight-icon star" />, text: "Interfaz Intuitiva" },
        { icon: <BulbOutlined className="highlight-icon bulb" />, text: "IA Integrada" },
        { icon: <CrownOutlined className="highlight-icon crown" />, text: "Resultados Profesionales" }
    ];

    return (
        <div className="welcome-container">
            {/* Área del título - arriba */}
            <div className="hero-title-area">
                <Title level={isMobile ? 1 : 1} className="hero-title-enhanced">
                    Evaluación de nota técnica
                    <br />
                    <span className="hero-subtitle">Potenciado con IA</span>
                </Title>
            </div>

            {/* Contenido inferior */}
            <div className="hero-bottom-content">
                <Paragraph className="hero-description-enhanced">
                    🎯 Transforma tus datos como un profesional
                    <br />
                    ⚡ Resultados instantáneos con tecnología avanzada
                    <br />
                    🤖 Asistente inteligente que entiende tus necesidades
                </Paragraph>

                <div className="hero-highlights">
                    {highlights.map((highlight, index) => (
                        <div key={index} className="highlight-item">
                            {highlight.icon}
                            <Text className="highlight-text">{highlight.text}</Text>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
};
