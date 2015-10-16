package io.vertxconcurrent;

import io.vertx.core.Context;

/**
 * Created by Rob Worsnop on 10/13/15.
 */
class ContextAction {
    private final Runnable action;
    private final Context context;

    ContextAction(Runnable action, Context context) {
        this.action = action;
        this.context = context;
    }

    void run(){
        context.runOnContext(v->action.run());
    }
}
