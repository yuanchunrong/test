import Vue from 'vue'
import App from './App.vue'

// 导入我们刚刚创建好的路由
import router from './router/index.js'

Vue.config.productionTip = false

new Vue({
  render: h => h(App),
  router
}).$mount('#app')
