"""A minimal component run as a real process by test_launcher_process.py."""
from fastapi.responses import Response

from besdk import launcher
from besdk.component import Definition
from besdk.http import Route


class Module:
    def routes(self, r):
        async def ok(c):
            return Response(status_code=204)

        r.add(Route("GET", "/api/ok", ok, public=True))


if __name__ == "__main__":
    launcher.main(Definition("demo/proc", "3.0.0", lambda rt: Module()))
