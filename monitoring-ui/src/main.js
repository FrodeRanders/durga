import { mount } from 'svelte'
import App from './App.svelte'

console.log('[durga] main.js loaded, mounting app')

const app = mount(App, {
  target: document.getElementById('app')
})

console.log('[durga] app mounted')

export default app
