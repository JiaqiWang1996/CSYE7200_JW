package controllers

import models.Main
import models.Main.form
import play.api.data.Form
import play.api.mvc._

import javax.inject._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(val controllerComponents: ControllerComponents) extends BaseController with play.api.i18n.I18nSupport {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  def index() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index(form: Form[Main])("No Result"))
  }

  def predictRes(res: String) = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index(form: Form[Main])(res))
  }

  def postMsg = Action { implicit request: Request[AnyContent] =>
    form.bindFromRequest.fold(
      formWithErrors => {
        // binding failure, you retrieve the form containing errors:
        BadRequest(views.html.index(formWithErrors)("No Result"))
      },
      main => {
        /* binding success, you get the actual value. */
        val res = Main.predict(main)
        Redirect(routes.HomeController.predictRes(res))
      }
    )
  }


}
