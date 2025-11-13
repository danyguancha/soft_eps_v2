# services/technical_note_services/report_service_aux/canvas_utils.py
import os
from reportlab.pdfgen import canvas

from services.technical_note_services.pdf_exporter_aux.pdf_config import (
    DEFAULT_IMAGE_HEIGHT,
    DEFAULT_IMAGE_WIDTH,
    DEFAULT_WATERMARK_OPACITY,
    IMAGE_POSITION_BOTTOM_LEFT,
    IMAGE_POSITION_BOTTOM_RIGHT,
    IMAGE_POSITION_CENTER,
    IMAGE_POSITION_TOP_LEFT,
    IMAGE_POSITION_TOP_RIGHT,
)


class NumberedCanvas(canvas.Canvas):
    """Canvas personalizado para agregar marca de agua y numeración"""

    def __init__(self, *args, **kwargs):
        self.watermark_text = kwargs.pop("watermark_text", None)
        self.watermark_image = kwargs.pop("watermark_image", None)
        self.watermark_opacity = kwargs.pop(
            "watermark_opacity", DEFAULT_WATERMARK_OPACITY
        )
        self.show_page_numbers = kwargs.pop("show_page_numbers", True)
        self.footer_text = kwargs.pop("footer_text", None)

        self.image_width = kwargs.pop("image_width", DEFAULT_IMAGE_WIDTH)
        self.image_height = kwargs.pop("image_height", DEFAULT_IMAGE_HEIGHT)
        self.image_position = kwargs.pop("image_position", IMAGE_POSITION_CENTER)

        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)

            if self.watermark_image:
                self.draw_watermark_image()

            if self.watermark_text:
                self.draw_watermark_text()

            self.draw_page_footer(self._pageNumber, num_pages)

            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_watermark_image(self):
        """Dibuja imagen como marca de agua"""
        if not self.watermark_image or not os.path.exists(self.watermark_image):
            return

        self.saveState()
        page_width, page_height = self._pagesize

        x, y = self._calculate_image_position(page_width, page_height)

        try:
            self.setFillAlpha(self.watermark_opacity)
            self.drawImage(
                self.watermark_image,
                x,
                y,
                width=self.image_width,
                height=self.image_height,
                mask="auto",
                preserveAspectRatio=True,
            )
        except Exception as e:
            print(f"Error dibujando marca de agua imagen: {e}")

        self.restoreState()

    def _calculate_image_position(self, page_width: float, page_height: float) -> tuple:
        """Calcula posición de la imagen según configuración"""
        position_map = {
            IMAGE_POSITION_CENTER: (
                (page_width - self.image_width) / 2,
                (page_height - self.image_height) / 2,
            ),
            IMAGE_POSITION_TOP_RIGHT: (
                page_width - self.image_width - 50,
                page_height - self.image_height - 50,
            ),
            IMAGE_POSITION_TOP_LEFT: (50, page_height - self.image_height - 50),
            IMAGE_POSITION_BOTTOM_RIGHT: (page_width - self.image_width - 50, 50),
            IMAGE_POSITION_BOTTOM_LEFT: (50, 50),
        }

        return position_map.get(
            self.image_position, position_map[IMAGE_POSITION_CENTER]
        )

    def draw_watermark_text(self):
        """Dibuja texto como marca de agua diagonal"""
        self.saveState()

        page_width, page_height = self._pagesize

        self.setFont("Helvetica-Bold", 60)
        self.setFillColorRGB(0.5, 0.5, 0.5, alpha=self.watermark_opacity)

        center_x = page_width / 2
        center_y = page_height / 2

        self.translate(center_x, center_y)
        self.rotate(45)

        text_width = self.stringWidth(self.watermark_text, "Helvetica-Bold", 60)
        self.drawString(-text_width / 2, 0, self.watermark_text)

        self.restoreState()

    def draw_page_footer(self, page_num: int, total_pages: int):
        """Dibuja footer con número de página y texto"""
        self.saveState()

        page_width = self._pagesize[0]

        if self.footer_text:
            self.setFont("Helvetica", 8)
            self.setFillColorRGB(0.3, 0.3, 0.3)
            self.drawString(30, 15, self.footer_text)

        if self.show_page_numbers:
            self.setFont("Helvetica", 8)
            self.setFillColorRGB(0.3, 0.3, 0.3)
            page_text = f"Página {page_num} de {total_pages}"
            text_width = self.stringWidth(page_text, "Helvetica", 8)
            self.drawString(page_width - text_width - 30, 15, page_text)

        self.restoreState()
