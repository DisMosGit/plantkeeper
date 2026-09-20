"""Automatic login for the local Django Admin.

The project has no application authentication: the REST API has no tokens and no
sessions, and this read-side UI is meant to be opened in a browser on a
developer's machine. Django Admin, however, renders as a user, records what that
user did and refuses to show anything to an anonymous request — so instead of a
login form this middleware selects one fixed local superuser on every request.

Three details make that safe rather than sloppy:

* the user's password is unusable, so even a hand-crafted POST to the stock login
  view cannot authenticate as anybody;
* ``DJANGO_AUTO_LOGIN_USER`` (empty) removes the middleware's effect and leaves
  Django's own login flow in place;
* the middleware lives in Django's sync request path, where the ORM is safe to
  call directly.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from django.conf import settings
from django.contrib.auth import get_user_model, login
from django.contrib.auth.models import AbstractBaseUser
from django.http import HttpRequest, HttpResponse

GetResponse = Callable[[HttpRequest], HttpResponse]
"""What Django hands a middleware: the rest of the chain."""


def ensure_auto_login_user() -> AbstractBaseUser:
    """Return the local superuser, creating it once if it does not exist.

    ``update_fields`` is deliberately narrow: the row may already carry a
    password the operator set, and only an unusable one is rewritten.
    """
    user_model = get_user_model()
    user = user_model.objects.get_or_create(
        username=settings.AUTO_LOGIN_USER,
        defaults={"is_staff": True, "is_superuser": True, "is_active": True},
    )[0]
    if user.has_usable_password():
        user.set_unusable_password()
        user.save(update_fields=["password"])
    return cast("AbstractBaseUser", user)


class DevAutoLoginMiddleware:
    """Sign every request in as the configured local user."""

    def __init__(self, get_response: GetResponse) -> None:
        self._get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        """Select the local user once per session, then run the view."""
        if settings.AUTO_LOGIN_USER and not request.user.is_authenticated:
            login(
                request,
                ensure_auto_login_user(),
                backend="django.contrib.auth.backends.ModelBackend",
            )
        return self._get_response(request)
