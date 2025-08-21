import jinja2
import pdfkit
WKHTMLTOPDF_PATH = "/usr/bin/wkhtmltopdf"
config = pdfkit.configuration(wkhtmltopdf=WKHTMLTOPDF_PATH)
data = {"soc_main_count": 1000}
template_loader = jinja2.FileSystemLoader(searchpath="../overall-report/")
template_env = jinja2.Environment(loader=template_loader)
template = template_env.get_template("overall-report_test.html")
html = template.render(data)
with open("test.html", "w") as f:
    f.write(html)

css_path = "../overall-report/base.min.css"

# set the PDF options with the path to your CSS file
options = {
    "user-style-sheet": css_path
}

pdfkit.from_file("test.html", "output.pdf", configuration=config, options=options)