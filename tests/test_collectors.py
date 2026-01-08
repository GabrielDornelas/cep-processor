"""
Unit tests for collectors module
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import requests

from src.collectors.web_scraper import WebScraper


class TestWebScraper:
    """Test cases for WebScraper class"""

    def test_init(self):
        """Test WebScraper initialization"""
        scraper = WebScraper(
            base_url="https://example.com",
            delay=1.0,
            max_ceps=100,
            timeout=10
        )
        
        assert scraper.base_url == "https://example.com"
        assert scraper.delay == 1.0
        assert scraper.max_ceps == 100
        assert scraper.timeout == 10
        assert len(scraper.collected_ceps) == 0

    def test_extract_cep(self):
        """Test CEP extraction from text"""
        scraper = WebScraper()
        
        # Test with hyphen
        text1 = "CEP: 01310-100"
        ceps1 = scraper._extract_cep(text1)
        assert "01310-100" in ceps1
        
        # Test without hyphen
        text2 = "CEP: 01310100"
        ceps2 = scraper._extract_cep(text2)
        assert "01310-100" in ceps2
        
        # Test multiple CEPs
        text3 = "CEPs: 01310-100, 01311-200, 01312-300"
        ceps3 = scraper._extract_cep(text3)
        assert len(ceps3) == 3
        assert "01310-100" in ceps3
        assert "01311-200" in ceps3
        assert "01312-300" in ceps3

    def test_is_valid_cep(self):
        """Test CEP validation - São Paulo CEPs must start with 0"""
        scraper = WebScraper()
        
        # Valid São Paulo CEPs (must start with 0)
        assert scraper._is_valid_cep("01310-100") is True
        assert scraper._is_valid_cep("01001-900") is True
        assert scraper._is_valid_cep("01234-567") is True
        
        # Invalid CEPs (don't start with 0 - not São Paulo CEPs)
        assert scraper._is_valid_cep("12345-678") is False  # Doesn't start with 0
        assert scraper._is_valid_cep("10001-220") is False  # Doesn't start with 0
        assert scraper._is_valid_cep("10101-009") is False  # Doesn't start with 0
        
        # Invalid format
        assert scraper._is_valid_cep("01310-10") is False  # Wrong length
        assert scraper._is_valid_cep("01310100") is False  # Missing hyphen
        assert scraper._is_valid_cep("abcde-fgh") is False  # Non-numeric
        assert scraper._is_valid_cep("") is False  # Empty

    @patch('src.collectors.web_scraper.requests.Session')
    def test_get_page_success(self, mock_session_class):
        """Test successful page fetch"""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.content = b'<html><body>Test</body></html>'
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        scraper = WebScraper()
        soup = scraper._get_page("https://example.com")
        
        assert soup is not None
        assert soup.find('body') is not None

    @patch('src.collectors.web_scraper.requests.Session')
    def test_get_page_error(self, mock_session_class):
        """Test page fetch with error"""
        mock_session = Mock()
        mock_session.get.side_effect = requests.RequestException("Connection error")
        mock_session_class.return_value = mock_session
        
        scraper = WebScraper()
        soup = scraper._get_page("https://example.com")
        
        assert soup is None

    def test_collect_ceps_from_page_table_structure(self):
        """Test CEP collection from page with table structure (id='ul_list')"""
        scraper = WebScraper()
        
        # Test with table structure (like neighborhood pages)
        html_content = """
        <html>
            <body>
                <table id="ul_list">
                    <tbody id="tbody_results">
                        <tr>
                            <td>
                                <a href="/pt-br/brasil/cep/01310-100/" title="CEP 01310-100">01310-100</a>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <a href="/pt-br/brasil/cep/01311-200/" title="CEP 01311-200">01311-200</a>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <a href="/pt-br/brasil/cep/01312-300/" title="CEP 01312-300">01312-300</a>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        ceps = scraper._collect_ceps_from_page(soup)
        
        assert len(ceps) == 3
        assert "01310-100" in ceps
        assert "01311-200" in ceps
        assert "01312-300" in ceps

    def test_collect_ceps_from_page_fallback(self):
        """Test CEP collection from page without table structure (fallback)"""
        scraper = WebScraper()
        
        # Test without table structure (fallback to text extraction)
        html_content = """
        <html>
            <body>
                <p>CEP: 01310-100</p>
                <div>01311-200</div>
                <span>01312-300</span>
            </body>
        </html>
        """
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        ceps = scraper._collect_ceps_from_page(soup)
        
        assert len(ceps) >= 3
        assert "01310-100" in ceps
        assert "01311-200" in ceps
        assert "01312-300" in ceps

    def test_save_to_csv(self, tmp_path):
        """Test CSV file creation - CEPs saved without hyphens (numbers only)"""
        scraper = WebScraper()
        scraper.collected_ceps = {"01310-100", "01311-200", "01312-300"}
        
        output_path = tmp_path / "test_ceps.csv"
        scraper._save_to_csv(output_path)
        
        assert output_path.exists()
        
        # Verify CSV content - CEPs should be saved without hyphens
        with open(output_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 4  # Header + 3 CEPs
            assert lines[0].strip() == "cep"
            # CEPs should be saved as numbers only (no hyphens)
            assert "01310100" in lines[1]
            assert "01311200" in lines[2]
            assert "01312300" in lines[3]
            # Verify no hyphens in saved CEPs
            for line in lines[1:]:
                assert "-" not in line.strip()

    @patch('src.collectors.web_scraper.time.sleep')
    @patch('src.collectors.web_scraper.WebScraper._get_page')
    @patch('src.collectors.web_scraper.WebScraper._collect_ceps_from_page')
    @patch('src.collectors.web_scraper.WebScraper._get_neighborhoods')
    def test_scrape_basic(self, mock_get_neighborhoods, mock_collect_ceps, 
                          mock_get_page, mock_sleep, tmp_path):
        """Test basic scraping functionality"""
        # Setup mocks
        from bs4 import BeautifulSoup
        # Use html.parser for tests (no system dependencies needed)
        mock_soup = BeautifulSoup('<html><body>Test</body></html>', 'html.parser')
        mock_get_page.return_value = mock_soup
        mock_collect_ceps.return_value = {"01310-100", "01311-200"}
        mock_get_neighborhoods.return_value = []
        
        scraper = WebScraper(max_ceps=2)
        output_path = tmp_path / "ceps.csv"
        
        result_path = scraper.scrape(output_path)
        
        assert result_path == output_path
        assert output_path.exists()
        assert len(scraper.collected_ceps) == 2

    def test_is_valid_neighborhood_link(self):
        """Test neighborhood link validation"""
        scraper = WebScraper()
        
        # Valid neighborhood links
        assert scraper._is_valid_neighborhood_link("/pt-br/brasil/sp/sao-paulo/aclimacao/") is True
        assert scraper._is_valid_neighborhood_link("https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/pinheiros/") is True
        
        # Invalid links
        assert scraper._is_valid_neighborhood_link("/pt-br/brasil/sp/sao-paulo/") is False  # Base page, no neighborhood
        assert scraper._is_valid_neighborhood_link("/pt-br/brasil/cep/01310-100/") is False  # CEP page
        assert scraper._is_valid_neighborhood_link("/pt-br/brasil/sp/sao-paulo/logradouro/rua-exemplo/") is False  # Logradouro
        assert scraper._is_valid_neighborhood_link("/blog/") is False  # Blog page
        assert scraper._is_valid_neighborhood_link("") is False  # Empty

    def test_is_same_domain(self):
        """Test domain validation"""
        scraper = WebScraper(base_url="https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/")
        
        # Same domain
        assert scraper._is_same_domain("https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/aclimacao/") is True
        
        # Different domain
        assert scraper._is_same_domain("https://example.com/pt-br/brasil/sp/sao-paulo/") is False

    def test_get_neighborhoods(self):
        """Test neighborhood link extraction"""
        scraper = WebScraper(base_url="https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/")
        
        html_content = """
        <html>
            <body>
                <ul id="ul_list">
                    <li><a href="https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/aclimacao/">Aclimação</a></li>
                    <li><a href="https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/pinheiros/">Pinheiros</a></li>
                    <li><a href="/pt-br/brasil/sp/sao-paulo/vila-madalena/">Vila Madalena</a></li>
                    <li><a href="/pt-br/brasil/cep/01310-100/">CEP</a></li>
                    <li><a href="/pt-br/brasil/sp/sao-paulo/logradouro/rua-exemplo/">Logradouro</a></li>
                </ul>
            </body>
        </html>
        """
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        neighborhoods = scraper._get_neighborhoods(soup)
        
        # Should find 3 valid neighborhood links (not CEP or logradouro)
        assert len(neighborhoods) == 3
        # Check that valid neighborhoods are included
        neighborhood_urls = [url.lower() for url in neighborhoods]
        assert any("aclimacao" in url for url in neighborhood_urls)
        assert any("pinheiros" in url for url in neighborhood_urls)
        assert any("vila-madalena" in url for url in neighborhood_urls)
