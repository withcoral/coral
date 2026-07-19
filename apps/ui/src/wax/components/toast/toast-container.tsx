import { Bounce, ToastContainer as ToastContainerComponent } from 'react-toastify'
import 'react-toastify/dist/ReactToastify.css'

import * as styles from './toast-container.css'

export function ToastContainer() {
  return (
    <ToastContainerComponent
      autoClose={5000}
      className={styles.toastContainer}
      closeButton={false}
      closeOnClick
      draggable
      hideProgressBar
      icon={false}
      newestOnTop={false}
      pauseOnFocusLoss
      pauseOnHover
      position="top-right"
      rtl={false}
      theme="dark"
      transition={Bounce}
    />
  )
}
