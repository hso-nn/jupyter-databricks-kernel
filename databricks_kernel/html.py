import uuid

import tabulate


def stacktrace(title, stacktrace):
    key = uuid.uuid4().hex
    return f"""
    <div class="jp-RenderedText" style="background-color: var(--jp-rendermime-error-background);">
        <div id="error-{key}-summary"><i id="error-{key}-icon" class="far fa-plus-square"></i>&nbsp;{title}</div>
        <pre id="error-{key}-stacktrace" style="display: none; font-size: var(--jp-code-font-size); background-color: var(--jp-rendermime-error-background);">{stacktrace}</pre>
        <script>
            setTimeout(function() {{
                var summary = document.getElementById("error-{key}-summary")
                var stacktrace = document.getElementById("error-{key}-stacktrace")
                var icon = document.getElementById("error-{key}-icon")
                summary.addEventListener('mousedown', function(e) {{
                    e.preventDefault();
                    if (stacktrace.style.display === "none") {{
                        stacktrace.style.display = ""
                        icon.classList.replace("fa-plus-square", "fa-minus-square")
                    }} else {{
                        stacktrace.style.display = "none"
                        icon.classList.replace("fa-minus-square", "fa-plus-square")
                    }}
                }}, false);
            }}, 1000)
        </script>
    </div>"""


def table(data, headers):
    table_html = tabulate.tabulate(data, headers, tablefmt="html")
    container = f"""<div
        class="jp-RenderedText"
        style="font-size: 10px; font-family: var(--jp-code-font-family); max-height: 300px;">
            {table_html}
        </div>"""
    return container
