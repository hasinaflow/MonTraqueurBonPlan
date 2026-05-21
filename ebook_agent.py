"""Agent IA pour transformer un PDF source en ebook PDF professionnel.

Fonctionnalités:
- Extraction du texte d'un PDF existant
- Génération IA d'un HTML éditorial premium (Gemini)
- Conversion HTML -> PDF via WeasyPrint
- Ajout optionnel d'images extraites du PDF source
"""

from __future__ import annotations

import base64
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List

from google import genai
from google.genai import types
from pypdf import PdfReader
import pypdfium2 as pdfium
import weasyprint


@dataclass
class EbookBuildResult:
    html_path: Path
    pdf_path: Path
    images_count: int


class EbookAIAgent:
    """Pipeline complet PDF source -> ebook PDF mis en page."""

    def __init__(self, api_key: str | None = None, model: str = "gemini-2.5-pro") -> None:
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY manquant. Ajoute-la dans les variables d'environnement.")
        self.client = genai.Client(api_key=self.api_key)
        self.model = model

    def build_ebook(self, input_pdf: str | Path, output_pdf: str | Path = "ebook_professionnel.pdf") -> EbookBuildResult:
        input_pdf = Path(input_pdf)
        output_pdf = Path(output_pdf)

        if not input_pdf.exists():
            raise FileNotFoundError(f"PDF introuvable: {input_pdf}")

        text_content = self._extract_text(input_pdf)
        image_data_uris = self._extract_images_as_data_uri(input_pdf)
        html = self._generate_professional_html(text_content, image_data_uris)

        html_path = output_pdf.with_suffix(".html")
        html_path.write_text(html, encoding="utf-8")

        self._compile_to_pdf(html, output_pdf)

        return EbookBuildResult(
            html_path=html_path,
            pdf_path=output_pdf,
            images_count=len(image_data_uris),
        )

    def _extract_text(self, pdf_path: Path) -> str:
        reader = PdfReader(str(pdf_path))
        pages = []
        for i, page in enumerate(reader.pages, start=1):
            text = (page.extract_text() or "").strip()
            if text:
                pages.append(f"\n\n--- PAGE {i} ---\n{text}")
        if not pages:
            raise ValueError("Aucun texte exploitable n'a été extrait du PDF source.")
        return "\n".join(pages)

    def _extract_images_as_data_uri(self, pdf_path: Path, max_images: int = 12) -> List[str]:
        """Rend une image par page (miniature HD) pour enrichir l'ebook."""
        pdf = pdfium.PdfDocument(str(pdf_path))
        uris: List[str] = []

        for idx in range(min(len(pdf), max_images)):
            page = pdf[idx]
            pil_image = page.render(scale=2.0).to_pil()
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=True) as tmp:
                pil_image.save(tmp, format="JPEG", quality=88, optimize=True)
                tmp.flush()
                tmp.seek(0)
                raw = tmp.read()
            encoded = base64.b64encode(raw).decode("utf-8")
            uris.append(f"data:image/jpeg;base64,{encoded}")

        return uris

    def _generate_professional_html(self, source_text: str, image_data_uris: List[str]) -> str:
        image_html = "\n".join(
            f'<figure class="source-figure"><img src="{uri}" alt="Illustration source"/></figure>'
            for uri in image_data_uris
        )

        system_instruction = """
Tu es un Directeur Éditorial IA spécialisé dans la production d'ebooks premium.
Tu dois transformer un contenu brut en un fichier HTML5+CSS professionnel prêt à imprimer.

RÈGLES OBLIGATOIRES:
1) Retourne UNIQUEMENT un document HTML complet (commence par <!DOCTYPE html> et finit par </html>).
2) Mets une couverture, une table des matières, des sections hiérarchisées, des encadrés, des citations et des listes.
3) Design premium : palette sobre, typographies élégantes, rythme de lecture excellent, marges généreuses.
4) Gestion pagination: @page A4 avec marges 18-20mm, veuves/orphelines, page-break-inside: avoid sur blocs importants.
5) Ajoute un en-tête/pied de page discret avec numéro de page CSS Paged Media.
6) Si des illustrations existent, intègre-les intelligemment avec légendes.
7) Aucune explication hors HTML.
"""

        user_prompt = f"""
Crée un ebook professionnel à partir du contenu PDF ci-dessous.

[ILLUSTRATIONS BASE64 À UTILISER]
{image_html or '<!-- aucune illustration -->'}

[CONTENU SOURCE]
{source_text}
"""

        response = self.client.models.generate_content(
            model=self.model,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.25,
            ),
        )
        html = (response.text or "").strip()

        if not html.startswith("<!DOCTYPE html>"):
            raise ValueError("Le modèle n'a pas retourné un HTML complet valide.")
        return html

    @staticmethod
    def _compile_to_pdf(html_content: str, output_pdf: Path) -> None:
        weasyprint.HTML(string=html_content).write_pdf(str(output_pdf))


if __name__ == "__main__":
    # Exemple:
    # export GEMINI_API_KEY="..."
    # python ebook_agent.py input.pdf output_ebook.pdf
    import argparse

    parser = argparse.ArgumentParser(description="Transforme un PDF source en ebook PDF premium via IA.")
    parser.add_argument("input_pdf", help="Chemin du PDF source")
    parser.add_argument("output_pdf", nargs="?", default="ebook_professionnel.pdf", help="Chemin du PDF de sortie")
    args = parser.parse_args()

    agent = EbookAIAgent()
    result = agent.build_ebook(args.input_pdf, args.output_pdf)
    print(f"✅ Ebook généré: {result.pdf_path}")
    print(f"✅ HTML intermédiaire: {result.html_path}")
    print(f"✅ Images intégrées: {result.images_count}")
